import asyncio
import aiohttp

import requests
import json
import pandas as pd
import ast
import datetime
import time

from typing import List, Dict



class Helper:
    def __init__(self, project_name:str, url: str = None):
        self.project_name = project_name
        
        url_tmp = None
        if url == None:
            url_tmp = f"http://feature-store-{project_name}-service.feast.svc.cluster.local:6566"
        else:
            url_tmp = url
        
        self.url = url_tmp
        response = requests.get(self.url + "/health")
        print(response)

    async def get_dataset(self, uid:str = None, feature_view:str = None, fields:str = None, filtred_params:str = None, time_limit:int = None):
        dataset = None
        limit = -1

        url = self.url + "/cache"
        
        print(url)

        if time_limit != None:
            limit = time_limit
        else: 
            limit = 1800 # 30 min
      
        if feature_view != None and uid != None:
            return "Only one key: feature_view or uid"

        elif feature_view != None:
            data = {"feature_view": feature_view}
            if fields != None:
                data["fields"] = fields
            if filtred_params != None:
                data["filtred_params"] = filtred_params

            response = requests.get(url, data=json.dumps(data))
            if response.status_code == 201:
                res = json.loads(response.text)
                uid = res["uid"]
                print("uid: " + str(uid))
                data_uid = {"uid": uid}
                start_ts = time.time()
                time.sleep(30)
                while True:
                    try:
                        response = requests.get(url, data=json.dumps(data_uid))
                        if response.status_code == 200:
                            dataset = response.text
                            break
                        elif response.status_code == 226:
                            current_ts = time.time()
                            diff_time = current_ts - start_ts
                            if diff_time > limit:
                                return f"Time limit (default 30 min) end. Rerty response with uid later. uid: {uid}"
                            else:
                                time.sleep(30)
                        elif 400 <= response.status_code < 500:
                            return f"Client error: {response.status_code} - {response.text}"
                        elif 500 <= response.status_code < 600:
                            return f"Server error: {response.status_code} - {response.text}"
                        else:
                            return "Undifided error"
                    except aiohttp.ClientError as e:
                        return f"Request failed: {e}"
                    except asyncio.TimeoutError:
                        return f"Request timed out"
                    except Exception as e:
                        return f"An unexpected error occurred: {e}"
            elif response.status_code == 200:
                # print(response.text)
                dataset = response.text
            else:
                return response.text

            rows = dataset.split("\n")

            cleaned_rows = [] 
            for row in rows:
                row = row.replace("\\ '", "{'").replace("\\'", "'").replace("\'", "\"")
                row = row.replace("None", "null").replace("' ", "'")
                try:
                    cleaned_rows.append(json.loads(row))
                except Exception:
                    continue
            return pd.DataFrame(cleaned_rows)
        elif uid != None:
            data_uid = {"uid": uid}
            start_ts = time.time()
            
            while True:
                try:
                    response = requests.get(url, data=json.dumps(data_uid))
                    if response.status_code == 200:
                        dataset = response.text
                        break
                    elif response.status_code == 226:
                        current_ts = time.time()
                        diff_time = current_ts - start_ts
                        if diff_time > limit:
                            return f"Time limit (default 30 min) end. Rerty response with uid later. uid: {uid}"
                        else:
                            time.sleep(30)
                    elif 400 <= response.status_code < 500:
                        return f"Client error: {response.status_code} - {response.text}"
                    elif 500 <= response.status_code < 600:
                        return f"Server error: {response.status_code} - {response.text}"
                    else:
                        return "Undifided error"
                except aiohttp.ClientError as e:
                    return f"Request failed: {e}"
                except asyncio.TimeoutError:
                    return f"Request timed out"
                except Exception as e:
                    return f"An unexpected error occurred: {e}"
                    
            rows = dataset.split("\n")

            cleaned_rows = [] 
            for row in rows:
                row = row.replace("\\ '", "{'").replace("\\'", "'").replace("\'", "\"")
                row = row.replace("None", "null").replace("' ", "'")
                try:
                    cleaned_rows.append(json.loads(row))
                except Exception:
                    continue
            return pd.DataFrame(cleaned_rows)
        else:
            return "Set one key: feature_view or uid"

    def get_on_demaind_features_df(self, features: list, entities: Dict[str, list[str]]):
        url = self.url + "/get-online-features"
        
        data = {
            "features": features,
            "entities": entities
        }
        dataset = None

        try:
            # async with aiohttp.ClientSession() as session:
            #     async with session.post(url, data=json.dumps(data)) as response:
            response = requests.post(url, data=json.dumps(data))
            if response.status_code == 200:
                dataset = response.text
            elif 400 <= response.status_code < 500:
                print(f"Client error: {response.status_code} - {response.text}")
            elif 500 <= response.status_code < 600:
                print(f"Server error: {response.status_code} - {response.text}")
            else:
                print(f"Unexpected error: {response.status}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None
        
        json_data = json.loads(dataset)
        return pd.DataFrame(list(zip(*[result["values"] for result in json_data["results"]])), columns=json_data["metadata"]["feature_names"])


    def get_on_demaind_features_json(self, features: list, entities: Dict[str, list[str]]):
        url = self.url + "/get-online-features"
        
        data = {
            "features": features,
            "entities": entities
        }
        dataset = None

        try:
            # async with aiohttp.ClientSession() as session:
            #     async with session.post(url, data=json.dumps(data)) as response:
            response = requests.post(url, data=json.dumps(data))
            if response.status_code == 200:
                dataset = response.text
            elif 400 <= response.status_code < 500:
                print(f"Client error: {response.status_code} - {response.text}")
            elif 500 <= response.status_code < 600:
                print(f"Server error: {response.status_code} - {response.text}")
            else:
                print(f"Unexpected error: {response.status}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None
        
        json_data = json.loads(dataset)
        return dict(zip(json_data['metadata']['feature_names'], [result['values'][0] for result in json_data['results']]))


    def stop_task(self, uid: str):
        url = self.url + "/stop-task"
        
        data = {
            "uid": uid
        }
        try:
            response = requests.post(url, data=json.dumps(data))
            if response.status_code == 200:
                return response.text
            elif 400 <= response.status_code < 500:
                print(f"Client error: {response.status_code} - {response.text}")
            elif 500 <= response.status_code < 600:
                print(f"Server error: {response.status_code} - {response.text}")
            else:
                print(f"Unexpected error: {response.status}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None
