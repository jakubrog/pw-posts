import json
import os
from typing import List

from marshmallow import fields, Schema

from google.cloud import bigquery
from google.oauth2 import service_account


class Post(Schema):
    id = fields.Str()
    title = fields.Str()
    content = fields.Str()
    author = fields.Str()
    dateTime = fields.DateTime()
    lastModified = fields.DateTime()


class Posts:
    def __init__(self):
        json_str = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        json_data = json.loads(json_str)
        json_data['private_key'] = json_data['private_key'].replace('\\n', '\n')
        credentials = service_account.Credentials \
            .from_service_account_info(json_data, scopes=["https://www.googleapis.com/auth/cloud-platform"])

        self.client = bigquery.Client(credentials=credentials)

    def get_all_posts(self) -> List[Post]:
        query = "SELECT * FROM `posts.posts` ORDER BY datetime DESC"
        results = self.client.query(query).result()
        return [Post().dump(row) for row in results]

    def get_post_by(self, id: str) -> Post:
        query = "SELECT * FROM `posts.posts` WHERE id = @id"
        query_parameters = [bigquery.ScalarQueryParameter("id", "STRING", id)]
        query_results = self.client.query(query, job_config=bigquery.QueryJobConfig(
            query_parameters=query_parameters)).result()
        for row in query_results:
            return Post().dump(row)
        raise ValueError("Post with given id not found")

    def update_post(self, post_id: str, title: str, content: str) -> bool:
        query = """
            UPDATE `posts.posts` SET 
                content = @content, 
                title = @title, 
                lastModified = CURRENT_TIMESTAMP()
            WHERE id = @id
            """
        query_parameters = [bigquery.ScalarQueryParameter("content", "STRING", content),
                            bigquery.ScalarQueryParameter("title", "STRING", title),
                            bigquery.ScalarQueryParameter("id", "STRING", post_id)]

        query_job = self.client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=query_parameters))
        query_job.result()
        return query_job.num_dml_affected_rows == 1

    def add_post(self, title: str, content: str, author: str) -> bool:
        query = """
            INSERT INTO `posts.posts`(id, content, title, author, dateTime) VALUES (
                GENERATE_UUID(), @content, @title, @author, CURRENT_TIMESTAMP()
            )
            """
        query_parameters = [bigquery.ScalarQueryParameter("content", "STRING", content),
                            bigquery.ScalarQueryParameter("title", "STRING", title),
                            bigquery.ScalarQueryParameter("author", "STRING", author)]

        query_job = self.client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=query_parameters))
        query_job.result()
        return query_job.num_dml_affected_rows == 1

    def delete_post(self, post_id: str) -> bool:
        query = """DELETE FROM  `posts.posts` WHERE id = @id"""
        query_parameters = [bigquery.ScalarQueryParameter("id", "STRING", post_id)]

        query_job = self.client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=query_parameters))
        query_job.result()
        return query_job.num_dml_affected_rows == 1
