import docker
import tarfile
import io
import os, time

class DockerFileHandler:
    def __init__(self, container_name_or_id, root_path="/var/lib/pgsql/13.1/data/"):
        self.client = docker.from_env()
        self.container = self.client.containers.get(container_name_or_id)
        self.root_path = root_path

    def get_file_path(self, file_name):
        return os.path.join(self.root_path, file_name)

    def write_file(self, file_name, content):
        """
        Write a file into the container
        (overwrite it if it exists, or create it if it does not).
        Principle: build a tar archive in memory and upload it via put_archive.
        """
        # 1. Prepare the data
        if isinstance(content, str):
            data = content.encode('utf-8')
        else:
            data = content  # Assume it is bytes

        container_path = self.get_file_path(file_name)

        # 2. Build an in-memory tar stream
        tar_stream = io.BytesIO()
        with tarfile.open(fileobj=tar_stream, mode='w') as tar:
            # Create a TarInfo object
            # (defines the file name, size, and permissions)
            tarinfo = tarfile.TarInfo(name=os.path.basename(container_path))
            tarinfo.size = len(data)
            tarinfo.mtime = int(time.time())  # Optional: set the modification time
            # Write the data into the tar archive
            tar.addfile(tarinfo, io.BytesIO(data))
        
        tar_stream.seek(0)  # Reset the pointer to the beginning

        # 3. Send it to Docker
        # The path parameter must be the parent directory of the target file;
        # Docker will extract the tar archive into that directory
        dir_path = os.path.dirname(container_path)
        
        # Ensure the directory exists
        # (optional, to avoid errors if the directory does not exist)
        # self.container.exec_run(f"mkdir -p {dir_path}")

        self.container.put_archive(
            path=dir_path,
            data=tar_stream
        )

        # [Key addition] Immediately fix the file ownership to postgres after writing
        # This step must be executed with root privileges
        # (exec_run is usually root by default unless USER is changed in the Dockerfile)
        fix_perm_cmd = f"chown postgres:postgres {container_path}"
        self.container.exec_run(fix_perm_cmd, user='root')

    def clear_file(self, file_name):
        """
        Clear the file content
        (keep the file, but set its size to 0).
        Principle: execute the shell command truncate.
        """
        # Using sh -c '> file' is the most general way to clear a file
        # (more compatible than the truncate command)
        container_path = self.get_file_path(file_name)
        cmd = f"sh -c '> {container_path}'"
        
        exit_code, output = self.container.exec_run(cmd)
        
        if exit_code == 0:
            print(f"[Clear] Successfully cleared: {container_path}")
        else:
            print(f"[Clear] Failed: {output.decode()}")

    def delete_file(self, file_name):
        """
        Delete a file.
        Principle: execute the shell command rm.
        """
        container_path = self.get_file_path(file_name)
        cmd = f"rm -f {container_path}"
        
        exit_code, output = self.container.exec_run(cmd)
        
        if exit_code == 0:
            print(f"[Delete] Successfully deleted: {container_path}")
        else:
            # If the file itself does not exist, rm -f usually does not raise an error;
            # this branch handles other errors such as permission issues
            print(f"[Delete] Failed: {output.decode()}")

    def read_file(self, file_name):
        """
        Read the file content
        (auxiliary functionality).
        """
        try:
            container_path = self.get_file_path(file_name)
            bits, stat = self.container.get_archive(container_path)
            
            # get_archive returns a tar stream, which needs to be unpacked
            file_content = io.BytesIO()
            for chunk in bits:
                file_content.write(chunk)
            file_content.seek(0)
            
            with tarfile.open(fileobj=file_content, mode='r') as tar:
                # Extract the first file in the tar archive
                member = tar.getmembers()[0]
                f = tar.extractfile(member)
                content = f.read().decode('utf-8')
                return content
        except Exception as e:
            print(f"[Read] Failed to read: {e}")
            return None
        
if __name__ == "__main__":
    pass