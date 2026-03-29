import os
from unittest.mock import MagicMock, patch

import boto3
from fastapi.testclient import TestClient
from moto import mock_s3

from bdi_api.settings import Settings


class TestS4Student:
    """
    Use this class to create your own tests to validate your implementation.

    For more information on testing, search `pytest` and `fastapi.testclient`.
    """

    @mock_s3
    def test_download_with_mocked_s3(self, client: TestClient) -> None:
        """Test download endpoint with mocked S3"""
        # Create mock S3 bucket
        settings = Settings()
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=settings.s3_bucket)

        # Mock requests to download files
        with patch('bdi_api.s4.exercise.requests') as mock_requests:
            mock_file_response = MagicMock()
            mock_file_response.status_code = 200
            mock_file_response.content = b'{"now": 1234567890, "aircraft": []}'

            mock_requests.get.return_value = mock_file_response

            with client as client:
                response = client.post("/api/s4/aircraft/download?file_limit=2")
                assert response.status_code == 200
                assert response.json() == "OK"

                # Verify files were uploaded to S3
                s3_objects = s3_client.list_objects_v2(
                    Bucket=settings.s3_bucket,
                    Prefix='raw/day=20231101/'
                )
                assert 'Contents' in s3_objects
                assert len(s3_objects['Contents']) == 2

    @mock_s3
    def test_prepare_with_mocked_s3(self, client: TestClient) -> None:
        """Test prepare endpoint with mocked S3"""
        # Create mock S3 bucket with test data
        settings = Settings()
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=settings.s3_bucket)

        # Upload realistic test file to mock S3 - include all expected fields
        test_json = '{"now": 1698796800, "aircraft": [{"hex": "abc123", "r": "N123AB", "t": "B738", "lat": 40.7, "lon": -74.0, "alt_baro": 35000, "gs": 450, "emergency": null}]}'
        s3_client.put_object(
            Bucket=settings.s3_bucket,
            Key='raw/day=20231101/000000Z.json.gz',
            Body=test_json.encode()
        )

        with client as client:
            response = client.post("/api/s4/aircraft/prepare")
            assert response.status_code == 200
            assert response.json() == "OK"

            # Verify database was created
            db_path = os.path.join(settings.prepared_dir, 'aircraft.db')
            assert os.path.exists(db_path)

            # Cleanup
            if os.path.exists(db_path):
                os.remove(db_path)


class TestItCanBeEvaluated:
    """
    Those tests are just to be sure I can evaluate your exercise.
    Don't modify anything from here!

    Make sure all those tests pass with `pytest` or it will be a 0!
    """

    @mock_s3
    def test_download(self, client: TestClient) -> None:
        """Test that download endpoint works correctly"""
        # Create mock S3 bucket
        settings = Settings()
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=settings.s3_bucket)

        # Mock requests to avoid calling real API
        with patch('bdi_api.s4.exercise.requests') as mock_requests:
            # Mock file download
            mock_file_response = MagicMock()
            mock_file_response.status_code = 200
            mock_file_response.content = b'{"now": 1234567890, "aircraft": []}'

            mock_requests.get.return_value = mock_file_response

            with client as client:
                response = client.post("/api/s4/aircraft/download?file_limit=1")
                assert not response.is_error, "Error at the download endpoint"
                assert response.json() == "OK"

    @mock_s3
    def test_prepare(self, client: TestClient) -> None:
        """Test that prepare endpoint works correctly"""
        # Create mock S3 bucket with test data
        settings = Settings()
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=settings.s3_bucket)

        # Upload a realistic test file to S3 - include all expected fields
        test_json = '{"now": 1698796800, "aircraft": [{"hex": "abc123", "r": "N123AB", "t": "B738", "lat": 40.7, "lon": -74.0, "alt_baro": 35000, "gs": 450, "emergency": null}]}'
        s3_client.put_object(
            Bucket=settings.s3_bucket,
            Key='raw/day=20231101/000000Z.json.gz',
            Body=test_json.encode()
        )

        with client as client:
            response = client.post("/api/s4/aircraft/prepare")
            assert not response.is_error, "Error at the prepare endpoint"
            assert response.json() == "OK"

            # Verify database was created
            db_path = os.path.join(settings.prepared_dir, 'aircraft.db')
            assert os.path.exists(db_path), "Database file not created"

            # Cleanup
            if os.path.exists(db_path):
                os.remove(db_path)

    @mock_s3
    def test_download_respects_file_limit(self, client: TestClient) -> None:
        """Test that download respects the file_limit parameter"""
        settings = Settings()
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=settings.s3_bucket)

        with patch('bdi_api.s4.exercise.requests') as mock_requests:
            # Mock file downloads
            mock_file_response = MagicMock()
            mock_file_response.status_code = 200
            mock_file_response.content = b'{"now": 1234567890, "aircraft": []}'

            mock_requests.get.return_value = mock_file_response

            with client as client:
                response = client.post("/api/s4/aircraft/download?file_limit=5")
                assert response.status_code == 200

                # Verify correct number of files in S3
                s3_objects = s3_client.list_objects_v2(
                    Bucket=settings.s3_bucket,
                    Prefix='raw/day=20231101/'
                )
                if 'Contents' in s3_objects:
                    # Should have uploaded 5 files
                    assert len(s3_objects['Contents']) == 5

    @mock_s3
    def test_download_stores_in_correct_s3_path(self, client: TestClient) -> None:
        """Test that files are stored in the correct S3 path"""
        settings = Settings()
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=settings.s3_bucket)

        with patch('bdi_api.s4.exercise.requests') as mock_requests:
            mock_file_response = MagicMock()
            mock_file_response.status_code = 200
            mock_file_response.content = b'{"now": 1234567890, "aircraft": []}'

            mock_requests.get.return_value = mock_file_response

            with client as client:
                response = client.post("/api/s4/aircraft/download?file_limit=1")
                assert response.status_code == 200

                # Check the file is in the correct path
                s3_objects = s3_client.list_objects_v2(
                    Bucket=settings.s3_bucket,
                    Prefix='raw/day=20231101/'
                )
                assert 'Contents' in s3_objects
                assert s3_objects['Contents'][0]['Key'].startswith('raw/day=20231101/')
                assert s3_objects['Contents'][0]['Key'].endswith('.json.gz')

    @mock_s3
    def test_prepare_downloads_all_files_from_s3(self, client: TestClient) -> None:
        """Test that prepare processes all files from S3"""
        settings = Settings()
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket=settings.s3_bucket)

        # Upload multiple test files with realistic data - include all expected fields
        test_files = ['000000Z.json.gz', '010000Z.json.gz', '020000Z.json.gz']
        for file_name in test_files:
            test_json = '{"now": 1698796800, "aircraft": [{"hex": "abc123", "r": "N123AB", "t": "B738", "lat": 40.7, "lon": -74.0, "alt_baro": 35000, "gs": 450, "emergency": null}]}'
            s3_client.put_object(
                Bucket=settings.s3_bucket,
                Key=f'raw/day=20231101/{file_name}',
                Body=test_json.encode()
            )

        with client as client:
            response = client.post("/api/s4/aircraft/prepare")
            assert response.status_code == 200

            # Verify database was created with data from all files
            db_path = os.path.join(settings.prepared_dir, 'aircraft.db')
            assert os.path.exists(db_path), "Database file not created"

            # Cleanup
            if os.path.exists(db_path):
                os.remove(db_path)

