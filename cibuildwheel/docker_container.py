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
    `with DockerContainer(docker_image = 'ubuntu') as docker:`

    A bash shell is running in the remote container. When `call()` is invoked,
    the command is relayed to the remote shell, and the results are streamed
    back to cibuildwheel.

    TODO:
        - [ ] Rename to OCI container as this now generalizes docker and
              podman?

    Example:
        >>> from cibuildwheel.docker_container import *  # NOQA
        >>> docker_image = "quay.io/pypa/manylinux2014_x86_64:2020-05-17-2f8ac3b"
        >>> # Test the default container
        >>> with DockerContainer(docker_image=docker_image) as self:
        ...     self.call(["echo", "hello world"])
        ...     self.call(["cat", "/proc/1/cgroup"])
        ...     print(self.get_environment())
    """

    UTILITY_PYTHON = "/opt/python/cp38-cp38/bin/python"

    process: PopenBytes
    bash_stdin: IO[bytes]
    bash_stdout: IO[bytes]

    def __init__(
        self,
        *,
        docker_image: str,
        simulate_32_bit: bool = False,
        cwd: Optional[PathOrStr] = None,
        oci_exe: str = "docker",
        env: Optional[Dict] = None,
    ):
        if not docker_image:
            raise ValueError("Must have a non-empty docker image to run.")

        self.docker_image = docker_image
        self.simulate_32_bit = simulate_32_bit
        self.cwd = cwd
        self.name: Optional[str] = None
        self.oci_exe = oci_exe
        self.env = env  # If specified, overwrite environment variables

    def __enter__(self) -> "DockerContainer":

        self.name = f"cibuildwheel-{uuid.uuid4()}"
        shell_args = ["linux32", "/bin/bash"] if self.simulate_32_bit else ["/bin/bash"]

        subprocess.run(
            [
                self.oci_exe,
                "create",
                "--env=CIBUILDWHEEL",
                f"--name={self.name}",
                "--interactive",
                # Z-flags is for SELinux
                "--volume=/:/host:Z",  # ignored on CircleCI
                self.docker_image,
                *shell_args,
            ],
            env=self.env,
            check=True,
        )

        self.process = subprocess.Popen(
            [
                self.oci_exe,
                "start",
                "--attach",
                "--interactive",
                self.name,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            env=self.env,
        )

        assert self.process.stdin and self.process.stdout
        self.bash_stdin = self.process.stdin
        self.bash_stdout = self.process.stdout

        # run a noop command to block until the container is responding
        self.call(["/bin/true"], cwd="")

        if self.cwd:
            # Although `docker create -w` does create the working dir if it
            # does not exist, podman does not. Unfortunately I don't think
            # there is a way to set the workdir on a running container.
            self.call(["mkdir", "-p", str(self.cwd)], cwd="")

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:

        self.bash_stdin.close()

        self.process.wait()

        assert isinstance(self.name, str)

        subprocess.run(
            [self.oci_exe, "rm", "--force", "-v", self.name],
            stdout=subprocess.DEVNULL,
            env=self.env,
        )
        self.name = None

    def copy_into(self, from_path: Path, to_path: PurePath) -> None:
        # `docker cp` causes 'no space left on device' error when
        # a container is running and the host filesystem is
        # mounted. https://github.com/moby/moby/issues/38995
        # Use `docker exec` instead.

        if from_path.is_dir():
            self.call(["mkdir", "-p", to_path])
            # NOTE: it may be necessary allow the user to exclude directories
            # (e.g. --exclude-vcs-ignores --exclude='.cache') in the future.
            # This is important if the oci images themselves are in the
            # repo directory we are copying into the container.
            subprocess.run(
                f"tar  cf - . | {self.oci_exe} exec -i {self.name} tar -xC {shell_quote(to_path)} -f -",
                shell=True,
                check=True,
                cwd=from_path,
                env=self.env,
            )
        else:
            subprocess.run(
                f'cat {shell_quote(from_path)} | {self.oci_exe} exec -i {self.name} sh -c "cat > {shell_quote(to_path)}"',
                shell=True,
                check=True,
                env=self.env,
            )

    def copy_out(self, from_path: PurePath, to_path: Path) -> None:
        # note: we assume from_path is a dir
        to_path.mkdir(parents=True, exist_ok=True)

        if self.oci_exe == "podman":
            command = f"{self.oci_exe} exec -i {self.name} tar -cC {shell_quote(from_path)} -f /tmp/output-{self.name}.tar ."
            subprocess.run(
                command,
                shell=True,
                check=True,
                cwd=to_path,
                env=self.env,
            )

            command = (
                f"{self.oci_exe} cp {self.name}:/tmp/output-{self.name}.tar output-{self.name}.tar"
            )
            subprocess.run(
                command,
                shell=True,
                check=True,
                cwd=to_path,
                env=self.env,
            )
            command = f"tar -xvf output-{self.name}.tar"
            subprocess.run(
                command,
                shell=True,
                check=True,
                cwd=to_path,
                env=self.env,
            )
            os.unlink(to_path / f"output-{self.name}.tar")
        elif self.oci_exe == "docker":
            command = f"{self.oci_exe} exec -i {self.name} tar -cC {shell_quote(from_path)} -f - . | tar -xf -"
            subprocess.run(
                command,
                shell=True,
                check=True,
                cwd=to_path,
                env=self.env,
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

        if cwd is None:
            # Hack because podman won't let us start a container with our
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
        self.bash_stdin.write(
            bytes(
                f"""(
            {chdir}
            env {env_assignments} {command}
            printf "%04d%s\n" $? {end_of_message}
        )
        """,
                encoding="utf8",
                errors="surrogateescape",
            )
        )
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
                output_io.write(line[0:footer_offset])
                break
            else:
                output_io.write(line)

        if isinstance(output_io, io.BytesIO):
            output = str(output_io.getvalue(), encoding="utf8", errors="surrogateescape")
        else:
            output = ""

        if returncode != 0:
            raise subprocess.CalledProcessError(returncode, args, output)

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

    def debug_info(self) -> str:
        if self.oci_exe == "podman":
            command = (f"{self.oci_exe} info --debug",)
        else:
            command = (f"{self.oci_exe} info",)
        completed: subprocess.CompletedProcess = subprocess.run(
            command,
            shell=True,
            check=True,
            cwd=self.cwd,
            env=self.env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        output = str(completed.stdout, encoding="utf8", errors="surrogateescape")
        return output


def shell_quote(path: PurePath) -> str:
    return shlex.quote(str(path))
