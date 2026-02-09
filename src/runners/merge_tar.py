"""Merge multiple files into a tar archive"""

import os
import sys
import tarfile

TAR_EXTENSIONS = ('.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz')

def merge_tar(output: str, inputs: list[str]) -> None:
    """Merge the input files into a tar.gz archive"""
    added = set()
    error = False
    with tarfile.open(output,"w:gz") as out_tar:
        for file in inputs:
            name = os.path.basename(file)
            if name.endswith(TAR_EXTENSIONS):
                print(f"Adding contents of tarball {name}:")
                with tarfile.open(file, "r:*") as in_tar:
                    for member in in_tar.getmembers():
                        if member.name in added:
                            print(f"  Found duplicate file {member.name}!")
                            error = True
                            continue
                        extracted = in_tar.extractfile(member)
                        if not extracted:
                            print(f"  Failed to extract {member.name} from {name}!")
                            error = True
                            continue
                        print(f"  {member.name}")
                        out_tar.addfile(member, extracted)
                        added.add(member.name)
            else:
                if name in added:
                    print(f"Found duplicate file {name}!")
                    error = True
                    continue
                print(f"Adding {name}")
                out_tar.add(file, name)
                added.add(name)
    if error:
        print("Errors were encountered during merging!")
        sys.exit(1)

if __name__ == "__main__":
    output_file = sys.argv[1]
    input_files = sys.argv[2:]
    merge_tar(output_file, input_files)
