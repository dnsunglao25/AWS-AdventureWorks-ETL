import boto3
import os

# 1. SETUP
s3_client = boto3.client('s3')
BUCKET_NAME = 'adventureworks-landing-458108203662'  # Same bucket, different folder
LOCAL_FILE = 'curated_adventure_works.csv'
S3_KEY = 'curated/curated_adventure_works.csv'  # Moving to the 'curated' prefix


def upload_gold_layer():
    try:
        if os.path.exists(LOCAL_FILE):
            print(f"Uploading {LOCAL_FILE} to Gold Layer (S3)...")

            # This uploads the file into the 'curated/' folder in your bucket
            s3_client.upload_file(LOCAL_FILE, BUCKET_NAME, S3_KEY)

            print("\n" + "="*40)
            print("PIPELINE COMPLETE!")
            print(
                f"Your curated data is now live at: s3://{BUCKET_NAME}/{S3_KEY}")
            print("="*40)
        else:
            print("Error: Curated file not found. Run transform_data.py first.")

    except Exception as e:
        print(f"Upload failed: {e}")


if __name__ == "__main__":
    upload_gold_layer()
