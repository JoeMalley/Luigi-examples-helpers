import luigi
import pandas as pd
from luigi.contrib.external_program import ExternalProgramTask
import polars as pl
from helpers.luigi_postgres import *
from helpers.data_report import *
from helpers.luigi_generic import *
import shlex
from os import listdir
from os.path import isfile, join
import pyarrow

class GlobalParams(luigi.Config):
    url = luigi.Parameter(default="localhost:8080")
    user = luigi.Parameter(default="admin")
    password = luigi.Parameter(default="admin")
    host = luigi.Parameter(default="localhost")
    database = luigi.Parameter(default="db_b4t")
    user = luigi.Parameter(default="b4tuser")
    password = luigi.Parameter(default="b4tuser1")


class collect_data_postgres(luigi.Task):
    def output(self):
        return luigi.LocalTarget('collectedData.csv')

    def requires(self):
        #Get all decoded msgs within a time frame
        return b4t_postgres_queryQuick(query="SELECT * FROM decoded_msg",code=887)

    def run(self):
        #sort the decoded messages and get the device info
        with self.input().open() as rows:
            devIds = []
            timestamps = []
            epochs = []
            rawDatas = []
            msgTypes = []
            msgSeq = []
            readings = []
            indexes = []
            FLOW1s = []
            FLOW2s = []
            FLOW3s = []
            FLOW4s = []
            FLOW5s = []
            FLOW6s = []
            FLOW7s = []
            FLOW8s = []
            xtimes = []
            MINFLOWs = []
            MAXFLOWs = []
            for i in rows:
                devIds.append(i[2]['devicename'])
                #timestamps.append(i[1])
                timestamps.append(str(i[1]).replace(" ","T"))
                epochs.append(i[2]['timestamp'])
                rawDatas.append(i[2]['data'])
                msgTypes.append(i[2]['decoded_data']['msgType'])
                msgSeq.append(i[2]['decoded_data']['msgSeq'])
                readings.append(i[2]['decoded_data']['readings'])
                indexes.append(i[2]['decoded_data']['index'])
                FLOW1s.append(i[2]['decoded_data']['FLOW1'])
                FLOW2s.append(i[2]['decoded_data']['FLOW2'])
                FLOW3s.append(i[2]['decoded_data']['FLOW3'])
                FLOW4s.append(i[2]['decoded_data']['FLOW4'])
                FLOW5s.append(i[2]['decoded_data']['FLOW5'])
                FLOW6s.append(i[2]['decoded_data']['FLOW6'])
                FLOW7s.append(i[2]['decoded_data']['FLOW7'])
                FLOW8s.append(i[2]['decoded_data']['FLOW8'])
                xtimes.append(i[2]['decoded_data']['xtime'])
                MINFLOWs.append(i[2]['decoded_data']['MINFLOW'])
                MAXFLOWs.append(i[2]['decoded_data']['MAXFLOW'])

            
            new_df_dict = {
                "DeviceID" : devIds,
                "Timestamp" : timestamps,
                "Epoch" : epochs,
                "rawData" : rawDatas,
                "msgType" : msgTypes,
                "msgSeq" : msgSeq,
                "readings" : readings,
                "index" : indexes,
                "FLOW1" : FLOW1s,
                "FLOW2" : FLOW2s,
                "FLOW3" : FLOW3s,
                "FLOW4" : FLOW4s,
                "FLOW5" : FLOW5s,
                "FLOW6" : FLOW6s,
                "FLOW7" : FLOW7s,
                "FLOW8" : FLOW8s,
                "xtime" : xtimes,
                "MAXFLOW" : MAXFLOWs,
                "MINFLOW" : MINFLOWs

            }
            df_out = pd.DataFrame(data=new_df_dict)
            df_out.to_csv(self.output().path,index=False)

class collect_data_bash(luigi.Task):
    def requires(self):
        #TODO need to provide different params ie date to actually get it to run a 2nd time
        return b4t_execute_shell_command(command="bash collect_data.sh srn devops rzBRWfBMh6NaccS 'o8bO#$nN0xbOssbER15/a6!ZXDWdkSAV7@PKCl^mb&rUpGReaxHLgiyie23$'")
    
    def output(self):
        return luigi.LocalTarget("collectedData.csv")
    def run(self):
        #TODO, get the final data, maybe the path should be returned from bash
        path = "./sh/finalData/srn"
        onlyfiles = [f for f in listdir(path) if isfile(join(path, f))]

        df_in = pd.read_csv(path + "/" +onlyfiles[0])
        onlyfiles.pop()
        for file in onlyfiles:
            df_in.append(pd.read_csv(path +"/" +file))
        
        df_in = df_in.drop_duplicates()

        df_in.to_csv(self.output().path,index=False)


class filter_dates(luigi.Task):
    start_date = luigi.Parameter(default='2022-09-01')
    end_date = luigi.Parameter(default='2022-09-30')
    def requires(self):
        return collect_data_bash()

    def output(self):
        return luigi.LocalTarget("collectedData_filtered.csv")
    def run(self):
        print("FILTERTING THE DATES")

        df_in = pl.scan_csv(self.input().path)
        df_in = df_in.filter(
            (pl.col("Timestamp") >= format_date(self.start_date))
            & (pl.col("Timestamp") <= format_date(self.end_date))
        )  # Filter the dataframe
        df_in = df_in.unique()#some reason distinct does not work
        df_in.collect().write_csv(self.output().path)

class initial_device_count(luigi.Task):
    start_date = luigi.Parameter(default='2022-09-01')
    end_date = luigi.Parameter(default='2022-09-30')
    def requires(self):
        return filter_dates(start_date=self.start_date, end_date=self.end_date)
    
    def output(self):
        return luigi.LocalTarget("initial_device_count.txt")
    def run(self):
        df_initial = pl.scan_csv(self.input().path).select(
            pl.col("DeviceID").unique().alias("device_id"),
        ).collect()

        count = df_initial.select(
            pl.col("device_id").count().alias("num_devices")
        ).get_column("num_devices")[0]

        with self.output().open("w") as file:
            file.write(str(count))



class format_report_data(luigi.Task):
    start_date = luigi.Parameter(default='2022-09-01')
    end_date = luigi.Parameter(default='2022-09-30')
    def requires(self):
        return filter_dates(start_date=self.start_date, end_date=self.end_date) 

    #outputs to a directory so we can have multiple outputs
    def output(self):
        return b4t_directory_target("./reportfiles")

    def run(self):
        check_dir(self.output().path)

        df_formatted = get_clean_data(self.input().path)
        df_clean_flows = get_flow_data(df_formatted)
        df_day_flows = get_day_flows(df_clean_flows)
        df_consecutive_flow_state = get_consecutive_flow_state(df_day_flows)
        
        df_consecutive_flow_state.write_csv(self.output().path + "/consecutive_flow.csv")
        df_day_flows.write_csv(self.output().path + "/day_flows.csv")


class leakage_report(luigi.Task):
    start_date = luigi.Parameter(default='2022-09-01')
    end_date = luigi.Parameter(default='2022-09-30') 

    def requires(self):
        return format_report_data(start_date=self.start_date, end_date=self.end_date)

    def output(self):
        return b4t_directory_target("./reportfiles/leakage")
    
    def run(self):
        #TODO a more maintanable way for directory targets, perhaps a subclass of the target that auto makes the dir
        check_dir(self.output().path)

        df_consecutive = pl.read_csv(self.input().path + "/consecutive_flow.csv")
        df_leak = get_leak(df_consecutive)
        df_leak_stats = get_leak_stats(df_leak)

        df_leak.write_csv(self.output().path + "/leakage.csv")
        df_leak_stats.write_csv(self.output().path + "/leakage_stats.csv")

class usage_report(luigi.Task):
    start_date = luigi.Parameter(default='2022-09-01')
    end_date = luigi.Parameter(default='2022-09-30')

    def requires(self):
        return format_report_data(start_date=self.start_date, end_date=self.end_date) 

    def output(self):
        return b4t_directory_target("./reportfiles/usage") 

    def run(self):
        check_dir(self.output().path)

        df_day_flows = pl.read_csv(self.input().path + "/day_flows.csv")
        df_usage = get_usage(df_day_flows)    
        df_usage_stats = get_usage_stats(df_usage)    

        df_usage.write_csv(self.output().path + "/usage.csv")
        df_usage_stats.write_csv(self.output().path + "/usage_stats.csv")


class dropped_report(luigi.Task):
    start_date = luigi.Parameter(default='2022-09-01')
    end_date = luigi.Parameter(default='2022-09-30')

    def requires(self):
        return [
            filter_dates(start_date=self.start_date, end_date=self.end_date),
            initial_device_count(start_date=self.start_date, end_date=self.end_date),
            usage_report(start_date=self.start_date, end_date=self.end_date)
        ]

    def output(self):
        return b4t_directory_target("./reportfiles/dropped")
    
    def run(self):
        check_dir(self.output().path)

        #TODO maybe a better way
        with self.input()[1].open("r") as i:
            count = i.read().splitlines()[0]
            print(self.input()[0].path)
            df_dropped = get_dropped_devices(self.input()[0].path)
            df_dropped.write_csv(self.output().path + "/dropped_devices.csv")
            
            df_usage = pl.scan_csv(self.input()[2].path + "/usage.csv")

            if(len(df_dropped) > 0):
                df_dropped_stats = get_dropped_device_stats(
                    df_dropped,df_usage,count
                )

                df_dropped_stats.write_csv(self.output().path + "/dropped_devices_stats.csv")
         

class execute_reports(luigi.Task):
    start_date = luigi.Parameter(default='2022-09-01')
    end_date = luigi.Parameter(default='2022-09-30')
    def requires(self):
        return [
            usage_report(start_date=self.start_date, end_date=self.end_date),
            leakage_report(start_date=self.start_date, end_date=self.end_date),
            dropped_report(start_date=self.start_date, end_date=self.end_date)
        ]
    
    def run(self):
        print("Reporting is finished")