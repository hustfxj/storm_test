#!/bin/bash

storm jar target/storm_perf_test-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.yahoo.storm.perftest.Main conf/test.prop
