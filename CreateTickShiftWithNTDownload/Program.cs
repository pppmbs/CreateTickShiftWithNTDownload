using CsvHelper;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CreateTickShiftWithNTDownload
{
    class Program
    {
        static class Constants
        {
            public const int TickCount = 2000; // ticks per bar
            //public const int barsLookAhear = 5; // look ahead 5 bars
            //public const int minBarRecords = 50; //anything less Some indicators, e.g. SMA50, MACD, will not have any value
            public const int slidingWindow = TickCount / 10; // sliding window to create multiple variations of the 2000 ticks bar records, i.e. expended the data size by 10x
                                                             //public const int slidingWindow = 97; // largest prime <100 sliding window to create multiple variations of the 2000 ticks bar records, i.e. expended the data size by 10x
            public const int slidingTotal = 10; // total number of sliding, i.e. # of augmented files generated
        }

        static public List<String> SplitESFileIntoDailyDataFiles(String esFile)
        {
            List<String> dailyDataFiles = new List<string>();
            string outputFileName;

            using (var sr = new StreamReader(esFile))
            {
                var reader = new CsvReader(sr, CultureInfo.InvariantCulture);

                //CSVReader will now read the entire es file into an enumerable
                IEnumerable records = reader.GetRecords<DataRecord>().ToList();

                List<DataRecord> listDataRecords = new List<DataRecord>();

                String startDate = "";
                bool startNewRecord = true;
                foreach (DataRecord record in records)
                {
                    if (startNewRecord || record.Date.Contains(startDate))
                    {
                        if (startNewRecord)
                        {
                            listDataRecords = new List<DataRecord>();
                            startNewRecord = false;
                            startDate = record.Date;
                        }

                        listDataRecords.Add(record);
                    }
                    else
                    {
                        outputFileName = Path.GetFileNameWithoutExtension(esFile) + "-" + startDate + ".csv";
                        using (var sw = new StreamWriter(outputFileName))
                        {
                            var writer = new CsvWriter(sw, CultureInfo.InvariantCulture);
                            writer.WriteRecords(listDataRecords);
                            writer.Flush();
                        }
                        dailyDataFiles.Add(outputFileName);
                        startNewRecord = true;
                    }
                }
                // flush last record
                outputFileName = Path.GetFileNameWithoutExtension(esFile) + "-" + startDate + ".csv";
                using (var sw = new StreamWriter(outputFileName))
                {
                    var writer = new CsvWriter(sw, CultureInfo.InvariantCulture);
                    writer.WriteRecords(listDataRecords);
                    writer.Flush();
                }
                dailyDataFiles.Add(outputFileName);
            }
            return dailyDataFiles;
        }

        static void Main(string[] args)
        {
            // To check the length of  
            // Command line arguments   
            if (args.Length == 0)
            {
                Console.WriteLine("CreateTickShiftWithNTDownload inputfiles");
                Environment.Exit(0);
            }

            foreach (string inESFile in args)
            {
                // first spilt into daily files
                List<String> dailyDataFiles = SplitESFileIntoDailyDataFiles(inESFile);
                IEnumerable inDailyDataFiles = dailyDataFiles;

                // the slidingTotal decides how many versions of tick shift data files are there
                for (int slidingNum = 1; slidingNum < Constants.slidingTotal; slidingNum++)
                {
                    string path = Constants.TickCount + "-NT-ticks-Shift\\" + Path.GetFileNameWithoutExtension(inESFile) + "." + slidingNum.ToString() + Path.GetExtension(inESFile);
                    File.Delete(path);

                    using (StreamWriter streamwriter = new StreamWriter(path, true, Encoding.UTF8, 65536))
                    {
                        // for each daily file, shift the ticks (shifting window decided by slidingNum*slidingWindow) and write the shifted data into ONE file for each tick shift version
                        foreach (String inFile in inDailyDataFiles)
                        {
                            using (var sr = new StreamReader(inFile))
                            {
                                var reader = new CsvReader(sr, CultureInfo.InvariantCulture);
                                List<DataRecord> ticks = new List<DataRecord>();

                                //CSVReader will now read the whole file into an enumerable
                                IEnumerable records = reader.GetRecords<DataRecord>().ToList();

                                int skip = 1;
                                foreach (DataRecord tick in records)
                                {
                                    if (skip > slidingNum * Constants.slidingWindow)
                                    {
                                        string bufString = tick.Date + " " + tick.Time + " " + tick.Fraction + ";" + tick.Bid + ";" + tick.Ask + ";" + tick.Volume;
                                        //File.AppendAllText(path, bufString + Environment.NewLine);
                                        streamwriter.WriteLine(bufString);
                                    }
                                    skip++;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
