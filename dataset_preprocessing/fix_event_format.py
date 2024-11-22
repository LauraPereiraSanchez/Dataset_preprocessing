from pathlib import Path
import argparse
import h5py
import numpy as np


# Define chunk size
chunk_size = 300000


def main():
    args = parse_arguments()

    directory = Path(args.input_path)
    file_names = [f.name for f in directory.iterdir() if f.is_file()]

    print(file_names)

    [ProcessFile(args.input_path+"/"+f, args.output_path+"/"+f) for f in file_names]
        
    return 

def parse_arguments():
    parser = argparse.ArgumentParser(description="A script for processing data.")
    parser.add_argument("input_path", type=str, help="Path to the input file")
    parser.add_argument("--output_path", type=str, help="Path to the output file", default="./")
    parser.add_argument("--chunk_size", type=int, help="Chunk size for processing", default=200000)

    return parser.parse_args()

def ProcessFile(input_file, output_file, chunk_size):

    dataset_name = "eventwise"
    
    # Open the input file and create the output file
    with h5py.File(input_file, "r") as f_in, h5py.File(output_file, "w") as f_out:
        for name in f_in:
            if name == dataset_name:
                # Process and modify the `eventwise` dataset
                event_dataset = f_in[name]
                total_size = event_dataset.shape[0]
                
                # Precompute total expanded size
                expanded_size = sum(event_dataset["n_jets"][:])

                # Create the modified dataset in the output file
                dset_out = f_out.create_dataset(
                    name, shape=(expanded_size,), dtype=event_dataset.dtype
                )
                
                current_index = 0

                # Process in chunks and modify
                for i in range(0, total_size, chunk_size):
                    chunk_end = min(i + chunk_size, total_size)
                    event_chunk = event_dataset[i:chunk_end]
                    n_jets_chunk = event_chunk["n_jets"]
                    
                    print(f"Processing chunk {i} to {chunk_end}")

                    # Expand the chunk
                    chunk_expanded = np.concatenate(
                        [np.repeat(event_chunk[j], n_jets_chunk[j]) for j in range(len(event_chunk))]
                    )

                    # Write modified data to the output dataset
                    dset_out[current_index : current_index + len(chunk_expanded)] = chunk_expanded
                    current_index += len(chunk_expanded)
            else:
                # Copy all other datasets as-is
                f_in.copy(name, f_out)

        print(f"All datasets copied, and {dataset_name} modified in {output_file}.")


if __name__ == "__main__":
    main()
