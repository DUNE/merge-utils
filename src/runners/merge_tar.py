"""Merge multiple files into a tar archive"""

import os
import sys
import tarfile

def merge_tar(output: str, inputs: list[str]) -> None:
    """Merge the input files into a tar.gz archive"""
    with tarfile.open(output,"w:gz") as tar:
        for file in inputs:
            name = os.path.basename(file)
            print(f"Adding {name}")
            tar.add(file, name)

if __name__ == "__main__":
    output_file = sys.argv[1]
    input_files = sys.argv[2:]
    merge_tar(output_file, input_files)
