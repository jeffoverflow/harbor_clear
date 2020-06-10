#!/usr/bin/env python

import argparse
import json
import logging
import pandas as pd
import requests
import re
import sys

logger = logging.getLogger(__name__)

class HarborClient(object):
    response_status = {
        200: "successfully.",    
        400: "Invalid.",
        401: "Unauthorized.",
        403: "Forbidden.",
        404: "Not found."
    }
 
    def __init__(self, schema, harbor_domain, username, password):
        self.schema = schema
        self.harbor_domain = harbor_domain
        self.harbor_url = self.schema + "://" + self.harbor_domain
        self.login_url = self.harbor_url + "/login"
        self.session = requests.Session()
        self.session.headers = {
            "Accept": "application/json"
        }
        self.session.auth = (username, password)

    def get_project_by_project_name(self, project_name):
        projs_url = self.harbor_url + "/api/projects?name=" + project_name
        resp = self.session.get(projs_url)

        success = resp.status_code == 200

        return {
            "success": success,
            "message": self.response_status.get(resp.status_code),
            "data": resp.json() if success else resp.text
        }

    def get_repos_by_project_id(self, project_id):
        repos_url = self.harbor_url + "/api/repositories/?project_id=" + str(project_id)
        resp = self.session.get(repos_url)

        success = resp.status_code == 200

        return {
            "success": success,
            "message": self.response_status.get(resp.status_code),
            "data": resp.json() if success else resp.text
        }

    def get_tags_by_repos(self, repo_name):
        tags_url = self.harbor_url + "/api/repositories/" + repo_name + "/tags"
        resp  = self.session.get(tags_url)

        success = resp.status_code == 200

        return {
            "success": success,
            "message": self.response_status.get(resp.status_code),
            "data": resp.json() if success else resp.text
        }

    def delete_image_by_tag_name(self, repo_name, tag_name):
        del_repo_tag_url = self.harbor_url + "/api/repositories/" + repo_name + "/tags/" + tag_name
        resp = self.session.delete(del_repo_tag_url)

        return {
            "success": resp.status_code == 200,
            "message": self.response_status.get(resp.status_code)
        }

def get_repos_tags_by_created_range(repos_tags, date_to=None, date_from=None):
    
    if date_to is None and date_from is None:
        return repos_tags

    created_date_to = pd.Timestamp.max.tz_localize('UTC')
    created_date_from = pd.Timestamp.min.tz_localize('UTC')

    if date_to is not None:
        created_date_to = date_to

    if date_from is not None:
        created_date_from = date_from

    tags = []

    for tag in repos_tags:
        tag_created_timestamp = tag['created'][:-1].ljust(26, '0')
        tag_created_timestamp = tag_created_timestamp[:19] + '.' + tag_created_timestamp[20:26]
        try:
            tag_created_date = pd.Timestamp(tag_created_timestamp, tz='UTC')
        except:
            logger.warning("Out of bounds nanosecond timestamp: {}".format(tag_created_timestamp))
            continue

        if created_date_to > tag_created_date and created_date_from <= tag_created_date:
            tags.append(tag)

    return tags

def get_repos_tags_by_name_regex_match(repos_tags, regex_str, flags=0):
    tags = []
    for tag in repos_tags:
        if re.match(regex_str, tag['name'], flags):
            tags.append(tag)
    return tags

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--schema',
        metavar='NAME',
        default='https',
        choices=['http', 'https'],
        help="http schema")
    parser.add_argument(
        '--harbor-domain',
        metavar='NAME',
        help="the host of Harbor service")
    parser.add_argument(
        '-u',
        '--username',
        default='admin',
        metavar='USERNAME',
        help="the username for accessing the registry instance")
    parser.add_argument(
        '-p',
        '--password',
        metavar='PASSWORD',
        help="the password for accessing the registry instance")
    parser.add_argument(
        '--project',
        metavar='NAME',
        help="the name of project")
    parser.add_argument(
        '--repository',
        metavar='NAME',
        help="the name of repository")
    parser.add_argument(
        '--regex',
        metavar='REGEX',
        help='only remove images regular matches to tag name')
    parser.add_argument(
        '--until',
        metavar='DURATION',
        help='only remove images created before given duration')
    parser.add_argument(
        '--data-to',
        metavar='TIMESTAMP',
        help='only remove images created before given timestamp')
    parser.add_argument(
        '--data-from',
        metavar='TIMESTAMP',
        help='only remove images created after given timestamp')

    args = parser.parse_args()

    if args.schema:
        schema = args.schema

    if args.harbor_domain:
        harbor_domain = args.harbor_domain

    if args.username:
        username = args.username

    if args.password:
        password = args.password

    proj_name = None
    if args.project:
        proj_name = args.project

    repo_name = None
    if args.repository:
        repo_name = args.repository

    regex_str = None
    if args.regex:
        regex_str = args.regex

    date_from_timestamp = None
    if args.data_from:
        date_from_timestamp = args.data_from

    date_to_timestamp = None
    if args.data_to:
        date_to_timestamp = args.data_to

    if args.until:
        date_to_timestamp = pd.Timestamp.utcnow() - pd.Timedelta(args.until)

    harbor_client = HarborClient(schema, harbor_domain, username, password)

    tags = []

    if repo_name:
        tags_result = harbor_client.get_tags_by_repos(repo_name)

        if not tags_result['success']:
            sys.exit("Get tags information failed!, " + tags_result['message'])
        else:
            if tags_result['data'] is None:
                sys.exit("Not found repository name: " + repo_name)

        tags = tags_result['data']

    elif proj_name:
        proj_result = harbor_client.get_project_by_project_name(proj_name)

        if not proj_result['success']:
            sys.exit("Get project information failed!, " + proj_result['message'])
        else:
            if proj_result['data'] is None:
                sys.exit("Not found project name: " + proj_name)

        proj_id = proj_result['data'][0]['project_id']

        repos_result = harbor_client.get_repos_by_project_id(proj_id)

        if not repos_result['success']:
            sys.exit("Get repository information failed!, " + repos_result['message'])
        else:
            if repos_result['data'] is None:
                sys.exit("Not found project id: " + proj_id)

        repos = repos_result['data']
        for repo in repos:
            repo_name = repo['name']
            
            tags_result = harbor_client.get_tags_by_repos(repo_name)

            if not tags_result['success']:
                sys.exit("Get tags information failed!, " + tags_result['message'])
            else:
                if tags_result['data'] is None:
                    sys.exit("Not found repository name: " + repo_name)

            tags = tags_result['data']

    tags = get_repos_tags_by_created_range(tags, date_to_timestamp, date_from_timestamp)

    if regex_str is not None:
        tags = get_repos_tags_by_name_regex_match(tags, regex_str)

    for tag in tags:
        result = harbor_client.delete_image_by_tag_name(repo_name, tag['name'])
        if result['success']:
            logger.info("success delete it.")
        else:
            logger.warning("delete failed!, message: {}".format(result['message']))
