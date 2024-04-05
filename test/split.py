import json
import random
import sys

def split_json_file(input_file, output_file1, output_file2, split_ratio=0.9):
    num_lines1 = 0
    num_lines2 = 0

    with open(input_file, 'r') as file, \
         open(output_file1, 'w') as file1, \
         open(output_file2, 'w') as file2:

        for line in file:
            if random.random() < split_ratio:
                file1.write(line)
                num_lines1 += 1
            else:
                file2.write(line)
                num_lines2 += 1

    print(f"JSON file split completed.")
    print(f"First file: {output_file1} ({num_lines1} lines)")
    print(f"Second file: {output_file2} ({num_lines2} lines)")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python script.py <input_file> <output_file1> <output_file2>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file1 = sys.argv[2]
    output_file2 = sys.argv[3]

    split_json_file(input_file, output_file1, output_file2)