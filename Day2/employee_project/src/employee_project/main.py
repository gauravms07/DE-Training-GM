import argparse
import pandas as pd

def clean_data(input_file):
    df = pd.read_csv(input_file)
    df.fillna(0, inplace=True)
    df.to_csv(input_file, index=False)
    

def filter_engineers(input_file, output_file):
    df = pd.read_csv(input_file)
    new_df = df[df['Department'] == "Engineering"]
    new_df.to_csv(output_file, index=False)
    print(output_file)

def main():
    parser = argparse.ArgumentParser(description="Arguments for problem 2")
    parser.add_argument("--input_file", required=True)
    parser.add_argument("--output_file", required=True)
    args = parser.parse_args()
    clean_data(args.input_file)
    filter_engineers(args.input_file, args.output_file)

if __name__ == "__main__":
    main()


        




