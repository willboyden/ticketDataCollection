﻿using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
//using ConsoleApp1.Properties;
using System.Configuration;
using System.Linq;
using static saveTicketMasterData.DO_NOT_GIT;

namespace ConsolsaveTicketMasterDataeApp1
{
	class saveTicketmasterDataProgram
	{
		
		private void writeVenueIdsToCsv(string sqlqry, string constr, string csvpath)
		{
			//string query = "SELECT distinct(id) as id from tblStubhubVenue";
			//string query = "SELECT distinct(id) as id from tblTicketmasterVenue";
			//@"C:\tempOutput\venueIDs\ticketmasterVenueIDs.csv"
			string connectionSql = Constr;
			StreamWriter myFile = new StreamWriter(csvpath);
			using (SqlConnection connection = new SqlConnection(constr))
			{
				SqlCommand command = new SqlCommand(sqlqry, connection);
				connection.Open();
				SqlDataReader reader = command.ExecuteReader();
				try
				{
					while (reader.Read())
					{
						myFile.WriteLine(String.Format("{0}", reader["id"] +","));
					}
				}
				catch (Exception ex)
				{
					Console.WriteLine(ex.ToString());
				}
				finally
				{
					reader.Close();
					myFile.Close();
				}
			}
		}
		static void Main(string[] args)
		{
			saveTicketmasterDataProgram p = new saveTicketmasterDataProgram();
			p.writeVenueIdsToCsv("SELECT distinct(id) as id from tblTicketmasterVenue", Constr, @"C:\tempOutput\venueIDs\ticketmasterVenueIDs.csv");
			p.getTicketmasterVenueEventJsonDump();
			p.writeTicketmasterVenueEventJsonToDB("tblTicketmasterVenueEvent", p.filePathsTicketmasterVenueEvent());//writes to new table
			p.updateTblNewTicketMasterVenueEvent("hour");//replaces old table

			//p.writeQueryToJson("select * from tblNewTicketMasterVenueEvent FOR JSON PATH", @"C:\Users\willb\source\repos\ticketDataCollection\outputdata\tblNewTicketMasterVenueEvent.json", "tblNewTicketMasterVenueEvent");
			//p.writeQueryToJson("select * from tblTicketMasterVenue FOR JSON PATH", @"C:\Users\willb\source\repos\ticketDataCollection\outputdata\tblTicketMasterVenue.json", "tblTicketMasterVenue");
			//p.writeQueryToCsv("select * from tblNewTicketMasterVenueEvent", @"C:\Users\willb\source\repos\ticketDataCollection\outputdata\tblNewTicketMasterVenueEvent.csv", "tblNewTicketMasterVenueEvent");
			//p.writeQueryToCsv("select * from tblTicketMasterVenue", @"C:\Users\willb\source\repos\ticketDataCollection\outputdata\tblTicketMasterVenue.csv", "tblTicketMasterVenue");
		}
		private readonly string constr = Constr;

		private void writeQueryToJson(string sqlqry, string jsonpath, string? tblName)//C:\Users\willb\source\repos\ticketDataCollection\outputdata
		{
			string connectionSql = Constr;
			StreamWriter myFile = new StreamWriter(jsonpath);
			using (SqlConnection connection = new SqlConnection(connectionSql))
			{
				SqlCommand command = new SqlCommand(sqlqry, connection);
				connection.Open();
				SqlDataReader reader = command.ExecuteReader();
				try
				{
					while (reader.Read())
					{
						myFile.Write(reader[0]);
					}
					var tblNme = tblName != null ? tblName : "table";
					Console.WriteLine($@"done writing {tblNme} to json file");
				}
				catch (Exception ex)
				{
					Console.WriteLine(ex.ToString());
				}
				finally
				{
					reader.Close();
					myFile.Close();
				}
			}
		}

		private void writeQueryToCsv(string sqlqry, string csvpath, string? tblName)//C:\Users\willb\source\repos\ticketDataCollection\outputdata
		{
			string connectionSql = Constr;
			StreamWriter myFile = new StreamWriter(csvpath);
			using (SqlConnection connection = new SqlConnection(connectionSql))
			{
				SqlCommand command = new SqlCommand(sqlqry, connection);
				connection.Open();
				SqlDataReader reader = command.ExecuteReader();
				try
				{
					object[] output = new object[reader.FieldCount];

					for (int i = 0; i < reader.FieldCount; i++)
						output[i] = reader.GetName(i);

					myFile.WriteLine(string.Join(",", output));

					while (reader.Read())
					{
						reader.GetValues(output);
						myFile.WriteLine(string.Join(",", output));
					}
					var tblNme = tblName != null ? tblName : "table";
					Console.WriteLine($@"done writing {tblNme} to file");
				}
				catch (Exception ex)
				{
					Console.WriteLine(ex.ToString());
				}
				finally
				{
					reader.Close();
					myFile.Close();
				}
			}
		}


		public string ticketmasterVenueJsonDirectory()
		{
			string[] subdirectoryEntries = Directory.GetDirectories(@"C:\tempOutput\ticketmasterJsonDumps\Venue");
			string dir = subdirectoryEntries.FirstOrDefault(p => p != "Archive");
			return dir;
		}
		public string ticketmasterVenueEventJsonDirectory()
		{
			string[] subdirectoryEntries = Directory.GetDirectories(@"C:\tempOutput\ticketmasterJsonDumps\Events");
			string dir = subdirectoryEntries.FirstOrDefault(p => p != "Archive");
			return dir;
		}	
		public string[] filePathsTicketmasterVenue()
		{
			return Directory.GetFiles(ticketmasterVenueJsonDirectory(), "*.txt",
										 SearchOption.TopDirectoryOnly);
		}
		public string[] filePathsTicketmasterVenueEvent()
		{
			return Directory.GetFiles(ticketmasterVenueEventJsonDirectory(), "*.txt",
										 SearchOption.TopDirectoryOnly);
		}
		
		private void run_pyScript(string fullFilepath)
		{
			Process p = new Process();
			p.StartInfo = new ProcessStartInfo(@"C:\\Users\\willb\\AppData\\Local\\Microsoft\\WindowsApps\\PythonSoftwareFoundation.Python.3.8_qbz5n2kfra8p0\python3.exe", fullFilepath)
			{
				WindowStyle = ProcessWindowStyle.Hidden,
				RedirectStandardOutput = true,
				UseShellExecute = false,
				CreateNoWindow = false,
				Verb = "runas"
			};
			p.Start();
			//Console.WriteLine(Constr);
			string output = p.StandardOutput.ReadToEnd();
			Console.WriteLine(output);
			p.WaitForExit();
			Console.WriteLine(output);


			Console.ReadLine();
		}

		private void getTicketmasterVenueJsonDump()
		{
			run_pyScript(@"C:\Users\willb\source\repos\ticketDataCollection\pythonScripts\ticketmasterVenueJsonToFile.py");
		}
		private void getTicketmasterVenueEventJsonDump()
		{
			run_pyScript(@"C:\Users\willb\source\repos\ticketDataCollection\pythonScripts\ticketmasterVenueEventJsonToFile.py");
		}
		private void updateTblNewTicketMasterVenueEvent(string srcapeDateSpecificty = "minute")
		{
			string specifity = "";
			if (srcapeDateSpecificty == "hour")
			{
				specifity = "CONCAT(DATEPART(DAY, scrapeDate), DATEPART(HOUR, scrapeDate))";
			}
			else
			{
				specifity = "CONCAT(DATEPART(DAY, scrapeDate), DATEPART(HOUR, scrapeDate), DATEPART(Minute, scrapeDate))";
			}
			string sqlstr = @$"
				DROP TABLE [stubhubApi].[dbo].[tblNewTicketMasterVenueEvent]
					SELECT [scrapeDate]
							  ,[id]
							  ,[name]
							  ,[url]
							  ,[sales_public_startDateTime]
							  ,[sales_public_endDateTime]
							  ,[dates_start_localTime]
							  ,[dates_start_dateTime]
							  ,[dates_status_code]
							  ,[links_self_href]
							  ,[seatmap_staticUrl]
							  ,[promoter_id]
							  ,[promoter_name]
							  ,[promoter_description]
							  ,[ticketLimit]
							 -- ,[classifications]
							--  ,[priceRanges]
							 -- ,[_embedded_venues]
							  ,[family]
							  ,[segment_name]
							  ,[genre_name]
							  ,[subGenre_name]
							  ,[priceRanges_min]
							  ,[priceRanges_max]
							  ,[venue_id]
						INTO [stubhubApi].[dbo].[tblNewTicketMasterVenueEvent]
						FROM [stubhubApi].[dbo].[tblTicketMasterVenueEvent]
					WHERE {specifity}
						IN (
							SELECT TOP 1  {specifity}
							FROM[stubhubApi].[dbo].[tblTicketMasterVenueEvent]
							GROUP BY scrapeDate, {specifity}
							ORDER BY scrapeDate DESC
						)";
			runNonQuery(sqlstr, Constr);
			Console.WriteLine("done updateing tblNewTicketMasterVenueEvent");

		}
		private void writeTicketmasterVenueEventJsonToDB(string tableName, string[] fpaths)
		{
			foreach (string f in fpaths)
			{
				string sqlstr = @"
						--TODO come back anddo comment below properly
						-- was checking schema for table name and changing query if it exits... this was hacky
						DECLARE @JSON VARCHAR(MAX)
						SELECT @JSON = BulkColumn
							FROM OPENROWSET 
							(BULK  '" + f + @"', SINGLE_CLOB) 
							AS j
							If (ISJSON(@JSON)=1)
								INSERT INTO " + tableName + @" -- for now... use SELECT INTO instead if it is the first time making table, see below
								Select GETDATE() as scrapeDate, *	
								-- INTO tblTicketmasterEvents_V8 --if first time making table
								FROM OPENJSON (@JSON, /* optional*/ '$.events') 
								WITH 
								(								
									 [id]  VARCHAR(20)
									 ,[name] VARCHAR(255)
									 --[type]		 
									 --[test]
									 ,[url] VARCHAR(255)
									 ,sales_public_startDateTime VARCHAR(25) '$.sales.public.startDateTime'
									 ,sales_public_endDateTime VARCHAR(25) '$.sales.public.endDateTime'
									 ,dates_start_localTime VARCHAR(25) '$.dates.start.localTime'
									 ,dates_start_dateTime VARCHAR(25) '$.dates.start.dateTime'
									 ,dates_status_code VARCHAR(25) '$.dates.status.code'
									 ,links_self_href VARCHAR(25) '$._links.self.href'
									 ,seatmap_staticUrl VARCHAR(100) '$.seatmap.staticUrl'
									 ,promoter_id  VARCHAR(25) '$.promoter.id'
									 ,promoter_name VARCHAR(25) '$.promoter.name'
									 ,promoter_description VARCHAR(75) '$.promoter.description'
									 ,ticketLimit VARCHAR(100) '$.ticketLimit.info'
									 ,classifications nvarchar(max) as JSON
									 ,priceRanges nvarchar(max) as JSON
									 
									 ,[_embedded_venues] nvarchar(max) '$._embedded.venues' as JSON 
								)
								cross apply OPENJSON (classifications)
								WITH 
								( 
									family varchar(255) '$.family'
									, segment_name VARCHAR(50) '$.segment.name'
									, genre_name VARCHAR(50) '$.genre.name'
									, subGenre_name VARCHAR(50) '$.subGenre.name'
								)
								outer apply OPENJSON (priceRanges)
								WITH 
								( 
									priceRanges_min varchar(25) '$.min'
									, priceRanges_max VARCHAR(25) '$.max'
								)
								outer apply OPENJSON ([_embedded_venues])
								WITH 
								( 
									venue_id varchar(255) '$.id'										
								)";
				runNonQuery(sqlstr, constr);
				Console.WriteLine("done  " + f);
			}
		}



		private void runNonQuery(string sqlstr, string constr)
		{
			try
			{
				using (SqlConnection connection = new SqlConnection(constr))
				{
					using (SqlCommand command = connection.CreateCommand())
					{
						connection.Open();
						//SqlParameter param = new SqlParameter();
						//param.ParameterName = "@p1";
						//param.Value = "'"+f+"'";
						//cmd.Parameters.Add(param);
						//command.Parameters.AddWithValue("@p0", f);
						//command.Parameters.Add(param);
						command.CommandText = sqlstr;
						//command.Connection.Open();
						command.ExecuteNonQuery();
						connection.Close();
						command.Dispose();
						
					}
				}
			}
			
			catch (Exception e)
			{
				File.AppendAllText("errorLog.txt", e.ToString());
			}
		}


	}
}

