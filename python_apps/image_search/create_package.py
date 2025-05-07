#!/usr/bin/env python3
import os
import zipfile
import sys

def create_package_zip(source_dir, output_zip):
    """Create a zip file containing all Python modules in source_dir."""
    with zipfile.ZipFile(output_zip, 'w') as zipf:
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                if file.endswith('.py'):
                    file_path = os.path.join(root, file)
                    # Get the relative path from source_dir
                    rel_path = os.path.relpath(file_path, source_dir)
                    zipf.write(file_path, rel_path)
    
    print(f"Created package zip at {output_zip}")
    return output_zip

if __name__ == "__main__":
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Create zip in the parent directory
    output_zip = os.path.join(script_dir, "image_search_package.zip")
    
    # Create the package zip
    create_package_zip(script_dir, output_zip)
