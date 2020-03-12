using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using static saveStubhubData.DO_NOT_GIT;

namespace saveStubhubData
{
    class saveStubhubDataProgram
    {
		
		public string jsonDirectoryStubhubVenueEvent()
		{
			string[] subdirectoryEntries = Directory.GetDirectories(@"C:\tempOutput\stubhubJsonDumps\Events");
			string dir = subdirectoryEntries.FirstOrDefault(p => p != "Archive");
			return dir;
		}		
		public string[] filePathsStubhubVenueEvent()
		{
			return Directory.GetFiles(jsonDirectoryStubhubVenueEvent(), "*.txt",
										 SearchOption.TopDirectoryOnly);
		}
		static void Main(string[] args)
		{
			saveStubhubDataProgram p = new saveStubhubDataProgram();
			p.writeVenueIdsToCsv("SELECT distinct(id) as id from tblStubhubVenue", Constr, @"C:\tempOutput\venueIDs\stubhubVenueIDs.csv");
			p.getStubhubVenueEventJsonDump();
			p.writeStubhubVenueEventJsonToDB("tblStubhubVenueEvent", p.filePathsStubhubVenueEvent());
			p.updateTblNewStubHubVenueEvent("minute");
		}
		private void writeVenueIdsToCsv(string sqlqry, string constr, string csvpath)
		{
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
						myFile.WriteLine(String.Format("{0}", reader["id"] + ","));
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
		private void run_pyScript(string fullFilepath)
		{
			Process p = new Process();
			p.StartInfo = new ProcessStartInfo(@"C:\\Users\\willb\\AppData\\Local\\Microsoft\\WindowsApps\\PythonSoftwareFoundation.Python.3.8_qbz5n2kfra8p0\python3.exe", fullFilepath)
			{
				RedirectStandardOutput = true,
				UseShellExecute = false,
				CreateNoWindow = false
			};
			p.Start();

			string output = p.StandardOutput.ReadToEnd();
			p.WaitForExit();

			Console.WriteLine(output);

			Console.ReadLine();
		}

		private void getStubhubVenueEventJsonDump()
		{
			run_pyScript(@"C:\Users\willb\source\repos\ticketDataCollection\pythonScripts\stubhubVenueEventJsonToFile.py");
		}
		private void getStubhubJsonToSql()
		{
			run_pyScript(@"C:\Users\willb\source\repos\ticketDataCollection\pythonScripts\stubhubAPIdataToDB.py");
		}

		private void writeStubhubVenueEventJsonToDB(string tableName, string[] fpaths)
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
								-- for now... use SELECT INTO instead if it is the first time making table, see below
							INSERT INTO " + tableName + @" -- for now... use SELECT INTO instead if it is the first time making table, see below
							Select 
							GETDATE() as scrapeDate
							,[name]
							,[id]
							,[status]
							,[description]
							,[hideEventDate]
							,[hideEventTime] 
							,[venue_id]
							,[category_name] 
							,[minListPrice] 
							,[maxListPrice] 
							,[totalTickets]
							,[totalListings]
							,
							CAST(
										replace
										(
											replace([eventDateLocal], 'T', ' ')
											, '-'
											, '.'
										) AS DATE
									) as [eventDate]
							--,
							--CAST(
							--		 replace
							--		 (
							--			 replace([eventDateUTC], 'T', ' ')
							--			 , '-'
							--			 , '.'
							--		 )
							--	 )
							,
							CAST(
								replace
								(
									replace([createdDate], 'T', ' ')
									, '+'
									, '.'
								) AS DATE
							) as [dateCreated]
							, 
							CAST(
										replace
										(
											replace([lastUpdatedDate], 'T', ' ')
											, '+'
											, '.'
										) AS DATE
							) as [lastUpdated]
							-- INTO tblTicketmasterEvents_V8 --if first time making table
							FROM OPENJSON (@JSON /* optional*/) 
							WITH (
							[name] VARCHAR(255) '$.name'
							,[id] VARCHAR(255) '$.id'
							,[status] VARCHAR(50) '$.status'
							--[locale] VARCHAR(255) '$.name'
							--[name] VARCHAR(255) '$.name'
							,[description] VARCHAR(255) '$.description'
							--[webURI] VARCHAR(255) '$.name'
							,[eventDateLocal] VARCHAR(255) '$.eventDateLocal'
							,[eventDateUTC] VARCHAR(255) '$.eventDateUTC'
							,[createdDate] VARCHAR(255) '$.createdDate'
							,[lastUpdatedDate] VARCHAR(255) '$.lastUpdatedDate'
							,[hideEventDate] VARCHAR(255) '$.hideEventDate'
							,[hideEventTime] VARCHAR(255) '$.hideEventTime'
							,[venue_id] VARCHAR(255) '$.venue.id'
							--[timezone] VARCHAR(255) '$.timezone'
							,[category_name] VARCHAR(255) '$.ancestors.category.name'
							--[currencyCode] VARCHAR(255) '$.name'
							,[minListPrice] VARCHAR(255) '$.ticketInfo.minListPrice'
							,[maxListPrice] VARCHAR(255) '$.ticketInfo.maxListPrice'
							,[totalTickets] VARCHAR(255) '$.ticketInfo.totalTickets'
							,[totalListings] VARCHAR(255) '$.ticketInfo.totalListings'
							)";
				runNonQuery(sqlstr, Constr);
				Console.WriteLine("done  " + f);
			}
		}

		private void updateTblNewStubHubVenueEvent(string srcapeDateSpecificty ="minute")
		{
			string specifity = "";
			if (srcapeDateSpecificty == "hour")
			{
				 specifity = "CONCAT(DATEPART(DAY, scrapeDate), DATEPART(HOUR, scrapeDate), DATEPART(Minute, scrapeDate))";
			}
			else
			{
				specifity = "CONCAT(DATEPART(DAY, scrapeDate), DATEPART(HOUR, scrapeDate))";
			}
			string sqlstr = @$"
				DROP TABLE [stubhubApi].[dbo].[tblNewStubHubVenueEvent]
					SELECT *
						INTO [stubhubApi].[dbo].[tblNewStubHubVenueEvent]
						FROM [stubhubApi].[dbo].[tblStubHubVenueEvent]
					WHERE {specifity}
						IN (
							SELECT TOP 1  {specifity}
							FROM[stubhubApi].[dbo].[tblStubHubVenueEvent]
							GROUP BY {specifity}
							ORDER BY {specifity} 
						)";
			runNonQuery(sqlstr, Constr);
			Console.WriteLine("done");
			
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
