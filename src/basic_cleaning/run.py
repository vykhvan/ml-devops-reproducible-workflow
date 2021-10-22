#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info("Downloading artifact")
    artifact_local_path  = run.use_artifact(args.input_artifact).file()
    
    # Load data
    logger.info("Loading data")
    data = pd.read_csv(artifact_local_path)
    
    # Drop outliers
    logger.info("Droping outliers")    
    min_price = args.min_price
    max_price = args.max_price
    idx = data['price'].between(min_price, max_price)
    data = data[idx].copy()
    
    # Convert last_review to datetime
    logger.info("Converting last_review to datetime") 
    data['last_review'] = pd.to_datetime(data['last_review'])
    
    # Drop rows in the dataset are not in the proper geolocation
    idx = data['longitude'].between(-74.25, -73.50) & data['latitude'].between(40.5, 41.2)
    data = data[idx].copy()

    # Save data
    logger.info("Saving data")
    data.to_csv("clean_sample.csv", index=False)

    # Create artifact instance
    logger.info("Creating artifact instance")
    artifact = wandb.Artifact(
         name=args.output_artifact,
         type=args.output_type,
         description=args.output_description,
    )

    # Upload artifact
    logger.info("Logging artifact")
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Fully-qualified name for the input artifact",
        required=True,
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name for the artifact",
        required=True,
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type for the artifact", 
        required=True,
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description for the artifact", 
        required=True,
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price for price column",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price for price column",
        required=True,
    )


    args = parser.parse_args()

    go(args)
