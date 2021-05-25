import io
import json
import os
import shlex
import subprocess
import sys
import uuid
from pathlib import Path, PurePath
from types import TracebackType
from typing import IO, Dict, List, Optional, Sequence, Type, cast

from .typing import PathOrStr, PopenBytes


class DockerContainer:
    """
    An object that represents a running Docker container.

    Intended for use as a context manager e.g.
    `with DockerContainer('ubuntu') as docker:`

    A bash shell is running in the remote container. When `call()` is invoked,
    the command is relayed to the remote shell, and the results are streamed
    back to cibuildwheel.

    Example:
        >>> from cibuildwheel.docker_container import *  # NOQA
        >>> docker_image = 'quay.io/pypa/manylinux_2_24_x86_64:2021-05-05-e1501b7'
        >>> with DockerContainer(docker_image) as self:
        ...     self.call(['echo', 'hello world'])
        ...     self.call(['cat', '/proc/1/cgroup'])
        ...     print(self.get_environment())

        >>> with DockerContainer(docker_image, oci_exe='podman') as self:
        ...     self.call(['echo', 'hello world'])
        ...     self.call(['cat', '/proc/1/cgroup'])
        ...     print(self.get_environment())
    """

    UTILITY_PYTHON = "/opt/python/cp38-cp38/bin/python"

    process: PopenBytes
    bash_stdin: IO[bytes]
    bash_stdout: IO[bytes]

    def __init__(
        self, docker_image: str, simulate_32_bit: bool = False, cwd: Optional[PathOrStr] = None,
        oci_exe: str = "docker", oci_root="",
    ):
        if not docker_image:
            raise ValueError("Must have a non-empty docker image to run.")

        self.docker_image = docker_image
        self.simulate_32_bit = simulate_32_bit
        self.cwd = cwd
        self.name: Optional[str] = None
        self.oci_exe = oci_exe
        self.oci_root = oci_root
        print('CREATE DOCKER OBJECT docker_image = {!r}'.format(docker_image))

    def __enter__(self) -> "DockerContainer":
        self.name = f"cibuildwheel-{uuid.uuid4()}"
        print('ENTER DOCKER OBJECT docker_image = {!r}, {}'.format(self.docker_image, self.name))

        # cwd_args = ["-w", str(self.cwd)] if self.cwd else []
        self.common_oci_args = []

        if self.oci_exe == 'docker':
            if self.oci_root != "":
                raise Exception('CIBW_DOCKER_ROOT only needed for podman')

        if self.oci_exe == 'podman':
            self.common_oci_args += [
                # https://stackoverflow.com/questions/30984569/error-error-creating-aufs-mount-to-when-building-dockerfile
                "--cgroup-manager=cgroupfs",
                "--storage-driver=vfs",
            ]
            if self.oci_root == "":
                # https://github.com/containers/podman/issues/2347
                self.common_oci_args += [
                    f"--root={os.environ['HOME']}/.local/share/containers/vfs-storage/",
                ]
            else:
                self.common_oci_args += [
                    f"--root={self.oci_root}",
                ]

        shell_args = ["linux32", "/bin/bash"] if self.simulate_32_bit else ["/bin/bash"]

        oci_create_args = []
        oci_start_args = []
        if self.oci_exe == 'podman':
            oci_create_args.extend([
                #https://github.com/containers/podman/issues/4325
                "--events-backend=file",
                "--privileged",
            ])
            oci_start_args.extend([
                "--events-backend=file",
            ])

        create_args = [
            self.oci_exe,
            "create",
            "--env=CIBUILDWHEEL",
            f"--name={self.name}",
            "--interactive",
            ] + oci_create_args + self.common_oci_args + [
            # Add Z-flags for SELinux
            "--volume=/:/host:Z",  # ignored on CircleCI
            # Removed becasue this does not work on podman if the workdir does
            # not already exist
            # *cwd_args,
            self.docker_image,
            *shell_args,
        ]
        print('create_args = {!r}'.format(' '.join(create_args)))
        subprocess.run(
            create_args,
            check=True,
        )

        self.common_docker_flags_join = ' '.join(self.common_oci_args)
        self.process = subprocess.Popen(
            [
                self.oci_exe,
                "start",
                "--attach",
                "--interactive",
            ] + oci_start_args + self.common_oci_args + [
                self.name,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )

        assert self.process.stdin and self.process.stdout
        self.bash_stdin = self.process.stdin
        self.bash_stdout = self.process.stdout

        # run a noop command to block until the container is responding
        self.call(["/bin/true"])

        if self.cwd:
            # Although `docker create -w` does create the working dir if it
            # does not exist, podman does not. Unfortunately I don't think
            # there is a way to set the workdir on a running container.
            self.call(["mkdir", "-p", str(self.cwd)])

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        import time

        # For podman this can output, adding a small sleep seems to mitigate it
        # open pidfd: No such process

        self.bash_stdin.close()

        if self.oci_exe == 'podman':
            time.sleep(0.01)

        self.process.terminate()
        self.process.wait()

        # Close stdin after termination seems to be more graceful with podman
        # self.bash_stdin = None
        # self.bash_stdout = None

        # When using podman there seems to be some race condition. Give it a
        # bit of extra time.
        if self.oci_exe == 'podman':
            time.sleep(0.01)

        assert isinstance(self.name, str)

        subprocess.run([self.oci_exe, "rm"] + self.common_oci_args + ["--force", "-v", self.name], stdout=subprocess.DEVNULL)

        self.name = None

    def copy_into(self, from_path: Path, to_path: PurePath) -> None:
        # `docker cp` causes 'no space left on device' error when
        # a container is running and the host filesystem is
        # mounted. https://github.com/moby/moby/issues/38995
        # Use `docker exec` instead.
        print(f'COPY INTO: {from_path} -> {to_path}')

        if from_path.is_dir():
            self.call(["mkdir", "-p", to_path])
            subprocess.run(
                f"tar --exclude-vcs-ignores cf - . | {self.oci_exe} exec {self.common_docker_flags_join} -i {self.name} tar -xC {shell_quote(to_path)} -f -",
                shell=True,
                check=True,
                cwd=from_path,
            )
        else:
            subprocess.run(
                f'cat {shell_quote(from_path)} | {self.oci_exe} exec {self.common_docker_flags_join} -i {self.name} sh -c "cat > {shell_quote(to_path)}"',
                shell=True,
                check=True,
            )

    def copy_out(self, from_path: PurePath, to_path: Path) -> None:
        # note: we assume from_path is a dir
        print(f'COPY OUT: {from_path} -> {to_path}')
        to_path.mkdir(parents=True, exist_ok=True)

        if self.oci_exe == 'podman':
            command = f"{self.oci_exe} exec {self.common_docker_flags_join} -i {self.name} tar -cC {shell_quote(from_path)} -f /tmp/output-{self.name}.tar ."
            subprocess.run(
                command,
                shell=True,
                check=True,
                cwd=to_path,
            )

            command = f"{self.oci_exe} cp {self.common_docker_flags_join} {self.name}:/tmp/output-{self.name}.tar output-{self.name}.tar"
            subprocess.run(
                command,
                shell=True,
                check=True,
                cwd=to_path,
            )

            command = f"tar -xvf output-{self.name}.tar"
            subprocess.run(
                command,
                shell=True,
                check=True,
                cwd=to_path,
            )

            os.unlink(to_path / f"output-{self.name}.tar")
        elif self.oci_exe == 'docker':
            command = f"{self.oci_exe} exec {self.common_docker_flags_join} -i {self.name} tar -cC {shell_quote(from_path)} -f - . | cat > output-{self.name}.tar"
            subprocess.run(
                command,
                shell=True,
                check=True,
                cwd=to_path,
            )
        else:
            raise KeyError(self.oci_exe)

    def glob(self, path: PurePath, pattern: str) -> List[PurePath]:
        glob_pattern = os.path.join(str(path), pattern)

        path_strs = json.loads(
            self.call(
                [
                    self.UTILITY_PYTHON,
                    "-c",
                    f"import sys, json, glob; json.dump(glob.glob({glob_pattern!r}), sys.stdout)",
                ],
                capture_output=True,
            )
        )

        return [PurePath(p) for p in path_strs]

    def call(
        self,
        args: Sequence[PathOrStr],
        env: Optional[Dict[str, str]] = None,
        capture_output: bool = False,
        cwd: Optional[PathOrStr] = None,
    ) -> str:

        # print('start call')

        if cwd is None:
            # Hack because podman wont let us start a container with our
            # desired working dir
            cwd = self.cwd

        chdir = f"cd {cwd}" if cwd else ""
        env_assignments = (
            " ".join(f"{shlex.quote(k)}={shlex.quote(v)}" for k, v in env.items())
            if env is not None
            else ""
        )
        command = " ".join(shlex.quote(str(a)) for a in args)
        end_of_message = str(uuid.uuid4())

        # log the command we're executing
        print(f"    + {command}")

        # Write a command to the remote shell. First we change the
        # cwd, if that's required. Then, we use the `env` utility to run
        # `command` inside the specified environment. We use `env` because it
        # can cope with spaces and strange characters in the name or value.
        # Finally, the remote shell is told to write a footer - this will show
        # up in the output so we know when to stop reading, and will include
        # the returncode of `command`.
        message = bytes(
                f"""(
            {chdir}
            env {env_assignments} {command}
            printf "%04d%s\n" $? {end_of_message}
        )
        """,
                encoding="utf8",
                errors="surrogateescape",
        )
        # print('message = {}'.format(message))
        self.bash_stdin.write(message)
        self.bash_stdin.flush()

        if capture_output:
            output_io: IO[bytes] = io.BytesIO()
        else:
            output_io = sys.stdout.buffer

        while True:
            line = self.bash_stdout.readline()

            if line.endswith(bytes(end_of_message, encoding="utf8") + b"\n"):
                # fmt: off
                footer_offset = (
                    len(line)
                    - 1  # newline character
                    - len(end_of_message)  # delimiter
                    - 4  # 4 returncode decimals
                )
                # fmt: on
                returncode_str = line[footer_offset : footer_offset + 4]
                returncode = int(returncode_str)
                # add the last line to output, without the footer
                part = line[0:footer_offset]
                output_io.write(part)
                break
            else:
                output_io.write(line)

        if isinstance(output_io, io.BytesIO):
            output = str(output_io.getvalue(), encoding="utf8", errors="surrogateescape")
        else:
            output = ""

        if returncode != 0:
            raise subprocess.CalledProcessError(returncode, args, output)

        # print('end call')

        return output

    def get_environment(self) -> Dict[str, str]:
        env = json.loads(
            self.call(
                [
                    self.UTILITY_PYTHON,
                    "-c",
                    "import sys, json, os; json.dump(os.environ.copy(), sys.stdout)",
                ],
                capture_output=True,
            )
        )
        return cast(Dict[str, str], env)

    def environment_executor(self, command: List[str], environment: Dict[str, str]) -> str:
        # used as an EnvironmentExecutor to evaluate commands and capture output
        return self.call(command, env=environment, capture_output=True)


def shell_quote(path: PurePath) -> str:
    return shlex.quote(str(path))
