import os
import json
import shutil
import logging
import traceback
from copy import deepcopy
from datetime import datetime, timedelta

from bottle import request, abort
from elasticsearch import Elasticsearch

from cirrus.lib.settings import Settings
from cirrus.webapps.lib.cirrusIndexing import CirrusIndex
from cirrus.webapps.lib.elasticsearch_lib import ESScrolledSearch

class TestcaseReporting(object):
    """
    This class is responsible for handling all the general reporting requests
    needed for the Test Results GUI page on Cirrus
    """

    cirrusIndex = CirrusIndex()
    cirrus_settings = Settings()

    def __init__(self):
        self.eclient = Elasticsearch()
        self.index = "cirrus"
        self.doc_type = "testinfo"
        self.cirrus_template_path = os.path.join(self.cirrus_settings['logstash_mappings_dir'],
                                                'cirrus_template.json')
        self.test_results_folder = os.path.join(self.cirrus_settings["webapps"]["user_test_results_folder"])
        self.ci_es_obj = ESScrolledSearch(index=self.index, doc_type=self.doc_type)
        self._create_base_filter_folder()
        self._intialize_cirrus_index()
        self.__save_view_structure = {
            "folder": None,
            "view": None,
            "filters": {}
        }

    def _intialize_cirrus_index(self):
        """
        Tries creating a Cirrus index in Elasticsearch with needed settings
        """
        try:
            # Create template structure
            with open(self.cirrus_template_path) as fobj:
                data = json.load(fobj)
                self.ci_es_obj.create_template("cirrus_template", data)
        except Exception:
            pass
        try:
            # Create index
            self.ci_es_obj.create_index(self.index)
        except Exception:
            pass

    def _get_aggregated_data(self, from_date, to_date, type):
        """
        Return aggregated data for needed fields to be plotted as graphs
        """
        query = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": {
                        "range": {
                            "jobdate": {
                                "gte":from_date,
                                "lte":to_date
                            }
                        }
                    }
                }
            },
            "aggs":{"jobstatus": {"terms":{"field" : "status", "size": 0}}}
        }
        if type:
            if type == "CPT":
                type = "L2ADD"
            query["query"]["bool"].update({
                "must": {
                    "match" : {
                        "type" : type
                    }
                }
            })

        fields = ["major","minor", "production", "program_type","testgroup"]
        sample_struct = {"terms":{"field" : "", "size": 0}}
        for field in fields:
            agg_struct = deepcopy(sample_struct)
            agg_struct["terms"]["field"] = "job_attrs.%s.raw"%field
            query["aggs"][field] = agg_struct
        out = self.eclient.search(index=self.index, doc_type=self.doc_type, body=query)
        data = out.get("aggregations", {})
        # Sample out data would be as:
        # {u'_shards': {u'failed': 0, u'successful': 5, u'total': 5},
         # u'aggregations': {u'major': {u'buckets': [{u'doc_count': 2412,
                                                    # u'key': u'Gen8'},
                                                   # {u'doc_count': 2000,
                                                    # u'key': u'NA'}],
                                      # u'doc_count_error_upper_bound': 0,
                                      # u'sum_other_doc_count': 0},
                           # u'minor': {u'buckets': [{u'doc_count': 4412,
                                                    # u'key': u'NA'}],
                                      # u'doc_count_error_upper_bound': 0,
                                      # u'sum_other_doc_count': 0},
                          # }
        # }
        aggr_data = {}
        for key, bucket in data.iteritems():
            aggr_data[key] = {}
            for doc in bucket["buckets"]:
                aggr_data[key][doc['key']] = doc['doc_count']

        return aggr_data

    def _get_date_filters(self, data, final_dict):
        """
        Returns the date range based on the type of filter
        """
        filter = data.get("filter")

        if  filter == "date":
            to_date = data["to_date"].split('T')[0]
            from_date = data["from_date"].split('T')[0]
            final_dict['filter'] = "%s to %s"%(from_date, to_date)
        elif filter == "days":
            days = data.get("days")
            to_date = datetime.now().strftime("%Y-%m-%d")
            from_date = (datetime.now() - timedelta(int(days))).strftime("%Y-%m-%d")
            final_dict['filter'] = "Last {0} days".format(days)
        elif filter == "all":
            to_date = None
            from_date = None
            final_dict['filter'] = "All"
        else:
            logging.warning("One of from/to date not provided."
                            "Fetching report details from past 1 month")
            to_date = datetime.now().strftime("%Y-%m-%d")
            from_date = (datetime.now() - timedelta(30)).strftime("%Y-%m-%d")

        final_dict["from_date"] = from_date
        final_dict["to_date"] = to_date
        return from_date, to_date

    def _form_query_dict(self, from_date, to_date, fields_visible):
        """
        Returns a query dict that needs to be fed to Elasticsearch
        """

        # Filter body for general test info
        query_dict = {
            "_source":{
                "excludes":["job_attrs.job_attrs_kv", "cust_attrs.cust_attrs_kv"]
            }
        }

        # Checking the provided date range inputs are available so as to form a
        # valid Elasticsearch query
        no_date_range = not from_date and not to_date
        both_date_ranges = from_date and to_date
        only_one_date_range = not from_date or not to_date

        if both_date_ranges:
            # Then we form a query to filter only that date range specific results
            # https://www.elastic.co/guide/en/elasticsearch/guide/current/combining-queries-together.html
            date_filter ={
                "query": {
                    "bool": {
                        "filter": {
                            "range": {
                                "jobdate": {
                                    "gte":from_date,
                                    "lte":to_date
                                }
                            }
                        }
                    }
                }
            }
            query_dict["query"] = date_filter
        elif no_date_range or only_one_date_range:
            # Then we can fetch all the available results
            logging.warn("Only one or no date ranges are provided. Will fetch all the data")

        query_dict["_source"]["includes"] = fields_visible + ["job_attrs", "cust_attrs"]
        return query_dict

    def get_test_case_data(self, data):
        """
        Return all report data or if needed of only specific type
        """
        data = data or request.json
        final_dict = {}
        final_dict["count"] = 0
        final_dict["testinfo"] = []
        final_dict['job_attributes'] = []
        final_dict["testattr"] = []
        final_dict['job_attributes_col'] = []
        final_dict['cirrus_attributes_col'] = []
        final_dict['xyz'] = []
        final_dict['testcase_attributes_col'] = []
        final_dict["reporttype"] = data.get("reporttype", "Testcase results")
        final_dict["retain"] = dict(data)
        final_dict["view_data"] = {}
        final_dict.update(dict(data))

        if "view_name" in data:
            view_name = data.get("view_name")
        # Attach folders path to the view name if available
        if "folders" in data:
            view_name = os.path.join(data["folders"], view_name)
        if "create_mode" in data:
            if self.is_a_duplicate_view_available(data["folders"], view_name):
                raise Exception("View name provided is duplicate for %s"%view_name)
        #final_dict["view_data"] = self.read_view_data(view_name, raise_except=False)

        try:
            # Finalizing date filtering
            from_date, to_date = self._get_date_filters(data, final_dict)

            #Main fields visible on GUI to fetch from Elasticsearch
            fields_visible = [
                "job", "job_id", "testcase_name", "status", "type", "jobdate", "executiontime",
                "log_uri", "stage", "sut", "sut_wwid", "log", "os", "model", "hardware", "team", 
                "spp_detail", "spp_status"
            ]

            # Get mapping of job and custom attributes
            cirrus_map_data = self.eclient.indices.get_mapping(index=self.index, doc_type=self.doc_type)
            if cirrus_map_data.get("cirrus"):
                job_attrs = cirrus_map_data[self.index]['mappings'][self.doc_type].get('properties', {}).get("job_attrs")
                cust_attrs = cirrus_map_data[self.index]['mappings'][self.doc_type].get('properties', {}).get("cust_attrs")
            else:
                job_attrs = {}
                cust_attrs = {}
            #Add job attrs and cust attrs fields to the visible list on GUI
            final_dict["cirrus_attributes_col"] = fields_visible[:]
            if job_attrs:
                job_attrs['properties'].pop("job_attrs_kv",None)
                fields_visible.extend(job_attrs["properties"].keys())
                final_dict["job_attributes_col"] = job_attrs["properties"].keys()
            if cust_attrs:
                cust_attrs['properties'].pop("cust_attrs_kv",None)
                fields_visible.extend(cust_attrs["properties"].keys())
                final_dict["testcase_attributes_col"] = cust_attrs["properties"].keys()
            final_dict["testattr"] = fields_visible

            # Create default row data so that GUI will have unique columns and values. Only the ones
            # identified from the elasticsearch will replace the column values in the row.
            default_row_data = dict((key,"NA") for key in fields_visible)

            # Form a query for filtering results
            query_dict = self._form_query_dict(from_date, to_date, fields_visible)
            test_info_data = self.ci_es_obj.search_index(query=query_dict)
            ring_data = self._get_aggregated_data(from_date, to_date, data.get("type"))
            ring_data["stage_status"] = ring_data.pop("jobstatus")

            # Rename/remove some fields to be seen from GUI
            indx = fields_visible.index("stage")
            final_dict["cirrus_attributes_col"][indx] = "stage_stdout"
            fields_visible.remove("log_uri")
            indx = fields_visible.index("status")
            fields_visible[indx] = "stage_status"
            final_dict["cirrus_attributes_col"][indx] = "stage_status"
            indx = fields_visible.index("type")
            final_dict["cirrus_attributes_col"][indx] = "test_type"

            # Iterate over documents, then fetch needed data from doc and store
            # it to return on to GUI
            cnt = 0
            for test_info in test_info_data:
                # Iterating through the test data
                for data in test_info:
                    row_data = deepcopy(default_row_data)
                    source = data["_source"]
                    job_attr_data = source.get("job_attrs", {})
                    row_data.update(job_attr_data)
                    cust_attr_data = source.get("cust_attrs", {})
                    row_data.update(cust_attr_data)
                    for field in source:
                        # Capture only necessary data
                        if field in row_data:
                            row_data[field] = source[field]
                        else:
                            continue

                        # Add some linked data for UI representation purpose
                        if field == "stage":
                            row_data["stage_stdout"] = row_data.pop("stage").split("_ci")[0]
                        elif field == "log_uri":
                            row_data["log_name"] = source[field].split('/')[-1]
                        elif field == "status":
                            row_data["job"] = source[field].capitalize()
                            row_data["stage_status"] = row_data.pop("status").capitalize()
                        elif field == "type":
                            row_data["test_type"] = row_data.pop("type")

                    cnt += 1
                    final_dict["testinfo"].append(row_data)

            final_dict["count"] = cnt
            final_dict["ring_data"] = ring_data
        except Exception as e:
            logging.error(traceback.format_exc())
            logging.error("Fetch testcase results failed")

        return final_dict

    def _create_base_filter_folder(self):
        try:
            if not os.path.exists(self.test_results_folder):
                os.mkdir(self.test_results_folder)
        except Exception:
            logging.debug("Test results filter folder pre exists")

    def _validate_save_view_data(self, data):
        """
        Checks whether the data uploaded to save is valid
        """
        missing_data = set(self.__save_view_structure) - set(data)
        unrequired_data = set(data) - set(self.__save_view_structure)
        if missing_data:
            raise Exception("Data to save the view is missing following needed info:" 
                            "%s"%(",".join(missing_data)))
        elif unrequired_data:
            raise Exception("Data to save the view is having following additional "
                            "uneeded info: %s"%(",".join(unrequired_data)))

    def __get_full_path(self, filter_data):
        folders = filter_data["folder"].strip()
        view_name = filter_data["view"].strip()
        return os.path.join(self.test_results_folder, folders, view_name+".json")

    def is_a_duplicate_view_available(self, folders, view_name):
        path = os.path.join(folders, view_name)
        data = self.get_available_views(path)
        if view_name in data["view"]:
            return True
        else:
            return False

    def __save_test_results_view(self, filter_data):
        """
        Saves the view of the Test results page independently or in to a specific
        provided folder
        """
        folders = filter_data.get("folder", "").strip()
        view_name = filter_data.get("view", "").strip()
        filters = filter_data.get("filters", "")
        save_path = ""
        if folders:
            logging.info("Creating folders for folders: %s"%folders)
            save_path = os.path.join(self.test_results_folder, folders)
            if not os.path.exists(save_path):
                os.makedirs(save_path)

        save_path = save_path or self.test_results_folder
        full_path = os.path.join(save_path, "%s.json"%view_name)
        with open(full_path, "wb") as fobj:
            json.dump(filters, fobj, indent=4)
        return filter_data

    def get_available_views(self, path=None):
        """
        Return  available test result views
        """
        if path:
            path = os.path.join(self.test_results_folder, path)
        else:
            path = self.test_results_folder

        ret_data = {"views": [], "folders": []}
        data = next(os.walk(path))
        ret_data["folders"] = data[1]
        ret_data["views"] = [fname.strip(".json") for fname in data[2]]
        return ret_data

    def read_view_data(self, view, raise_except=True):
        """
        Read filter data from a file
        """
        data = {}
        if view:
            view += ".json"
            path = os.path.join(self.test_results_folder, view)
        else:
            raise Exception("No view name provided to fetch data from")

        if os.path.exists(path) and not os.path.isfile(path):
            if raise_except:
                raise Exception("Provided view %s is not present or it is a folder type"%view)
        else:
            with open(path) as fobj:
                data = json.load(fobj)

        return data

    def create_test_results_view(self, filter_data, save_type):
        """
        Creates a new view
        """
        self._validate_save_view_data(filter_data)
        full_path = self.__get_full_path(filter_data)

        # Create only if the path doesen't exist already
        if os.path.exists(full_path):
            raise IOError("Provided view is a duplicate. A view with same name already"
                          "exists. %s"%os.path.join(folders, view_name))
        return self.__save_test_results_view(filter_data)

    def update_test_results_view(self, filter_data):
        """
        Updates an existing view
        """
        self._validate_save_view_data(filter_data)
        full_path = self.__get_full_path(filter_data)
        return self.__save_test_results_view(filter_data)

    def delete_test_results_view(self, path):
        """
        Deletes a particular view/folder
        """
        path = os.path.join(self.test_results_folder, path)
        if os.path.isfile(path):
            os.remove(path)
        else:
            shutil.rmtree(path)

    def move_test_results_view(self, src_path, dest_path):
        """
        Moves views/folders from one location to other
        """
        full_src_path = os.path.join(self.test_results_folder, src_path)
        full_dest_path = os.path.join(self.test_results_folder, dest_path)
        src_is_a_file = os.path.isfile(full_src_path+".json")
        src_exists = os.path.exists(full_src_path)
        dest_exists = os.path.exists(full_dest_path)
        logging.error(full_src_path)
        logging.error(full_dest_path)

        if not dest_exists:
            raise IOError("Destination path %s does not exist"%dest_path)
        elif src_is_a_file:
            full_src_path += ".json"
        elif not src_exists:
            raise IOError("Source path %s does not exist"%src_path)

        logging.error(full_dest_path)
        shutil.move(full_src_path, full_dest_path)
