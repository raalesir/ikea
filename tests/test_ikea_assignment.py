#!/usr/bin/env python

"""Tests for `ikea_assignment` package."""

import pytest


from ikea_assignment.ikea_assignment import BasicPipeline


import json


from ikea_assignment import  spark

class TestBasic:
    INPUT = "data/input/dataset.jsonl"




    def test_line_count_raw(self):
        """
        tests  the number of lines in the initial file == 9999 +1 for the header
        :return:
        """
        pipeline = BasicPipeline(self.INPUT, spark)

        assert pipeline.read_dataset().count() == 10000


    def test_line_count_processed_dataset(self):
        """
        tests the number of lines in the processed dataset
        :return:
        """
        pipeline = BasicPipeline(self.INPUT, spark)

        assert pipeline.transform().count() == 30009


    def test_number_of_items_by_two_methods(self):
        """
        testing that during the  transformation the we did not  loose any product,
        by comparing the set of all procuct  IDs
        using two different methods:  plain Python and  PySpark.

        This test should not be used for big dataset, just for a toy one.
        :return:
        """
        pipeline = BasicPipeline(self.INPUT, spark)

        print(pipeline.df.printSchema())
        data = []
        with open(self.INPUT) as f:
            for line in f:
                data.append(json.loads(line))

        items_by_python = set([key for row  in data  for elem  in row['items'] for key in elem])

        items_by_pyspark = set([id_[0] for id_ in pipeline.transform().select(['ID']).collect()])

        assert items_by_python == items_by_pyspark

