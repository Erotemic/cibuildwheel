"""
Invocation:
    pytest unit_test/docker_container_test.py --run-docker -v -s
    pytest unit_test/docker_container_test.py --run-docker -v -s -k "test_file_operations"
    pytest unit_test/docker_container_test.py --run-docker -v -s -k "test_no_lf"
    pytest unit_test/docker_container_test.py --run-docker -v -s -k "test_simple"
"""
import platform
import random
import shutil
import subprocess
import textwrap
from pathlib import Path, PurePath

import pytest

from cibuildwheel.docker_container import DockerContainer
from cibuildwheel.environment import EnvironmentAssignmentBash

# for these tests we use manylinux2014 images, because they're available on
# multi architectures and include python3.8
pm = platform.machine()
if pm == "x86_64":
    DEFAULT_IMAGE = "quay.io/pypa/manylinux2014_x86_64:2020-05-17-2f8ac3b"
elif pm == "aarch64":
    DEFAULT_IMAGE = "quay.io/pypa/manylinux2014_aarch64:2020-05-17-2f8ac3b"
elif pm == "ppc64le":
    DEFAULT_IMAGE = "quay.io/pypa/manylinux2014_ppc64le:2020-05-17-2f8ac3b"
elif pm == "s390x":
    DEFAULT_IMAGE = "quay.io/pypa/manylinux2014_s390x:2020-05-17-2f8ac3b"


def basis_container_kwargs():
    """
    Parametarize different container engine invocations.
    """
    HAVE_DOCKER = shutil.which("docker") != ""
    if HAVE_DOCKER:
        yield {"oci_exe": "docker", "docker_image": DEFAULT_IMAGE}
    HAVE_PODMAN = shutil.which("podman") != ""
    if HAVE_PODMAN:
        # Basic podman usage
        yield {"oci_exe": "podman", "docker_image": DEFAULT_IMAGE}
        # VFS Podman usage (for the podman in docker use-case)
        home = str(Path("~").expanduser())
        yield {
            "oci_exe": "podman",
            "oci_extra_args_common": f"--cgroup-manager=cgroupfs --storage-driver=vfs --root={home}/.local/share/containers/vfs-storage",
            "oci_extra_args_create": "--events-backend=file --privileged",
            "oci_extra_args_start": "--events-backend=file --cgroup-manager=cgroupfs --storage-driver=vfs",
            "docker_image": DEFAULT_IMAGE,
        }


@pytest.mark.docker
@pytest.mark.parametrize("container_kwargs", basis_container_kwargs())
def test_simple(container_kwargs):
    with DockerContainer(**container_kwargs) as container:
        assert container.call(["echo", "hello"], capture_output=True) == "hello\n"


@pytest.mark.docker
@pytest.mark.parametrize("container_kwargs", basis_container_kwargs())
def test_no_lf(container_kwargs):
    with DockerContainer(**container_kwargs) as container:
        assert container.call(["printf", "hello"], capture_output=True) == "hello"


@pytest.mark.docker
@pytest.mark.parametrize("container_kwargs", basis_container_kwargs())
def test_environment(container_kwargs):
    with DockerContainer(**container_kwargs) as container:
        assert (
            container.call(
                ["sh", "-c", "echo $TEST_VAR"], env={"TEST_VAR": "1"}, capture_output=True
            )
            == "1\n"
        )


@pytest.mark.docker
@pytest.mark.parametrize("container_kwargs", basis_container_kwargs())
def test_cwd(container_kwargs):
    with DockerContainer(cwd="/cibuildwheel/working_directory", **container_kwargs) as container:
        assert container.call(["pwd"], capture_output=True) == "/cibuildwheel/working_directory\n"
        assert container.call(["pwd"], capture_output=True, cwd="/opt") == "/opt\n"


@pytest.mark.docker
@pytest.mark.parametrize("container_kwargs", basis_container_kwargs())
def test_container_removed(container_kwargs):
    with DockerContainer(**container_kwargs) as container:
        docker_containers_listing = subprocess.run(
            f"{container.oci_exe} container {container._common_args_join} ls",
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            universal_newlines=True,
        ).stdout
        assert container.name is not None
        assert container.name in docker_containers_listing
        old_container_name = container.name

    docker_containers_listing = subprocess.run(
        f"{container.oci_exe} container {container._common_args_join} ls",
        shell=True,
        check=True,
        stdout=subprocess.PIPE,
        universal_newlines=True,
    ).stdout
    assert old_container_name not in docker_containers_listing


@pytest.mark.docker
@pytest.mark.parametrize("container_kwargs", basis_container_kwargs())
def test_large_environment(container_kwargs):
    # max environment variable size is 128kB
    long_env_var_length = 127 * 1024
    large_environment = {
        "a": "0" * long_env_var_length,
        "b": "0" * long_env_var_length,
        "c": "0" * long_env_var_length,
        "d": "0" * long_env_var_length,
    }

    with DockerContainer(**container_kwargs) as container:
        # check the length of d
        assert (
            container.call(["sh", "-c", "echo ${#d}"], env=large_environment, capture_output=True)
            == f"{long_env_var_length}\n"
        )


@pytest.mark.docker
@pytest.mark.parametrize("container_kwargs", basis_container_kwargs())
def test_binary_output(container_kwargs):
    with DockerContainer(**container_kwargs) as container:
        # note: the below embedded snippets are in python2

        # check that we can pass though arbitrary binary data without erroring
        container.call(
            [
                "/usr/bin/python2",
                "-c",
                textwrap.dedent(
                    """
                    import sys
                    sys.stdout.write(''.join(chr(n) for n in range(0, 256)))
                    """
                ),
            ]
        )

        # check that we can capture arbitrary binary data
        output = container.call(
            [
                "/usr/bin/python2",
                "-c",
                textwrap.dedent(
                    """
                    import sys
                    sys.stdout.write(''.join(chr(n % 256) for n in range(0, 512)))
                    """
                ),
            ],
            capture_output=True,
        )

        data = bytes(output, encoding="utf8", errors="surrogateescape")

        for i in range(512):
            assert data[i] == i % 256

        # check that environment variables can carry binary data, except null characters
        # (https://www.gnu.org/software/libc/manual/html_node/Environment-Variables.html)
        binary_data = bytes(n for n in range(1, 256))
        binary_data_string = str(binary_data, encoding="utf8", errors="surrogateescape")
        output = container.call(
            ["python2", "-c", 'import os, sys; sys.stdout.write(os.environ["TEST_VAR"])'],
            env={"TEST_VAR": binary_data_string},
            capture_output=True,
        )
        assert output == binary_data_string


@pytest.mark.docker
@pytest.mark.parametrize("container_kwargs", basis_container_kwargs())
def test_file_operation(tmp_path: Path, container_kwargs):
    with DockerContainer(**container_kwargs) as container:
        # test copying a file in
        test_binary_data = bytes(random.randrange(256) for _ in range(1000))
        original_test_file = tmp_path / "test.dat"
        original_test_file.write_bytes(test_binary_data)

        dst_file = PurePath("/tmp/test.dat")

        container.copy_into(original_test_file, dst_file)

        output = container.call(["cat", dst_file], capture_output=True)
        assert test_binary_data == bytes(output, encoding="utf8", errors="surrogateescape")


@pytest.mark.docker
@pytest.mark.parametrize("container_kwargs", basis_container_kwargs())
def test_dir_operations(tmp_path: Path, container_kwargs):
    with DockerContainer(**container_kwargs) as container:
        test_binary_data = bytes(random.randrange(256) for _ in range(1000))
        original_test_file = tmp_path / "test.dat"
        original_test_file.write_bytes(test_binary_data)

        # test copying a dir in
        test_dir = tmp_path / "test_dir"
        test_dir.mkdir()
        test_file = test_dir / "test.dat"
        shutil.copyfile(original_test_file, test_file)

        dst_dir = PurePath("/tmp/test_dir")
        dst_file = dst_dir / "test.dat"
        container.copy_into(test_dir, dst_dir)

        output = container.call(["cat", dst_file], capture_output=True)
        assert test_binary_data == bytes(output, encoding="utf8", errors="surrogateescape")

        # test glob
        assert container.glob(dst_dir, "*.dat") == [dst_file]

        # test copy dir out
        new_test_dir = tmp_path / "test_dir_new"
        container.copy_out(dst_dir, new_test_dir)

        assert test_binary_data == (new_test_dir / "test.dat").read_bytes()


@pytest.mark.docker
@pytest.mark.parametrize("container_kwargs", basis_container_kwargs())
def test_environment_executor(container_kwargs):
    with DockerContainer(**container_kwargs) as container:
        assignment = EnvironmentAssignmentBash("TEST=$(echo 42)")
        assert assignment.evaluated_value({}, container.environment_executor) == "42"
