from pathlib import Path

current_dir = Path.cwd()
current_file = Path(__file__).name

print(f"Files in {current_dir}:")

for filepath in current_dir.iterdir(): # returns all items in the drectory
    if filepath.name == current_file: # if the file being inspected is this scrpt,skip it
        continue

    print(f"  - {filepath.name}")

    if filepath.is_file(): # ensures  python only tries to read actual files and avoids errors.
        content = filepath.read_text(encoding='utf-8')
        print(f"    Content: {content}")