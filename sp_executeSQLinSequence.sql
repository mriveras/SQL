IF OBJECT_ID (N'dbo.sp_executeSQLinSequence') IS NOT NULL
	DROP PROCEDURE dbo.sp_executeSQLinSequence
GO

CREATE PROCEDURE [dbo].[sp_executeSQLinSequence] 
(
	 @groupName         VARCHAR(50) = 'ALL'
	,@SQLID             INT         = 0
	,@executionID       BIGINT      = 0
	,@startLogTreeLevel TINYINT     = 0
	,@debug             BIT         = 0
	,@loggingType       BIT         = 1 --1) Table | 2) DataGovernor | 3) Table & DataGovernor
)
/*
	Developed by: Mauricio Rivera Senior
	Date: 19 Oct 2018
	
	MODIFICATIONS
		
		
	LAST USED LOGGING IDS:
		- ERROR       (COD-E)
		- WARNING     (COD-W)
		- INFORMATION (COD-I)
*/
AS
BEGIN
	--checking sequence
		IF(
			NOT EXISTS(
				SELECT 1
				FROM sys.sequences 
				WHERE name = 'sq_BI_log_executionID'
			)
		)
			BEGIN
				CREATE SEQUENCE dbo.sq_BI_log_executionID
			    	START     WITH 1  
			    	INCREMENT BY   1; 
			END
	
	IF(OBJECT_ID('dbo.BI_log') IS NULL)
		BEGIN
			CREATE TABLE dbo.BI_log
			(
				executionID BIGINT         NOT NULL,
				sequenceID  INT            NOT NULL,
				logDateTime DATETIME       NOT NULL,
				object      VARCHAR (256)  NOT NULL,
				scriptCode  VARCHAR (25)   NOT NULL,
				status      VARCHAR (50)   NOT NULL,
				message     VARCHAR (500)  NOT NULL,
				SQL         VARCHAR (max)  NOT NULL,
				SQL2        VARCHAR (max)  NOT NULL,
				SQL3        VARCHAR (max)  NOT NULL,
				variables   VARCHAR (2500) NOT NULL,
			);
			CREATE CLUSTERED INDEX CIX_dbo_BIlog_1 ON dbo.BI_log (executionID);
		END
	
	IF OBJECT_ID (N'dbo.BI_sqlToExecute') IS NULL
		BEGIN
			CREATE TABLE dbo.BI_sqlToExecute
				(
					SQLID                 INT IDENTITY                NOT NULL,
					groupName             VARCHAR (50)                NOT NULL,
					destinationSchemaName NVARCHAR (128) DEFAULT ('') NOT NULL,
					destinationTableName  NVARCHAR (128) DEFAULT ('') NOT NULL,
					sequenceOrder         SMALLINT       DEFAULT (1)  NOT NULL,
					SQL1                  NVARCHAR (max) DEFAULT ('') NOT NULL,
					SQL2                  NVARCHAR (max) DEFAULT ('') NOT NULL,
					SQL3                  NVARCHAR (max) DEFAULT ('') NOT NULL,
					disabled              BIT            DEFAULT (0)  NOT NULL,
					CONSTRAINT PK_BI_sqlToExecute PRIMARY KEY (SQLID)
				);
			CREATE INDEX CIX_dbo_BIsqlToExecute_1 ON dbo.BI_sqlToExecute (groupName);
		END
	
	--Declaring User Table for Log purpose
		DECLARE @BI_log TABLE (			
			 executionID BIGINT
			,sequenceID  INT IDENTITY(1,1)
			,logDateTime DATETIME
			,object      VARCHAR (256)
			,scriptCode  VARCHAR (25)
			,status      VARCHAR (50)
			,message     VARCHAR (500)
			,SQL         VARCHAR (max)
			,SQL2        VARCHAR (max)
			,SQL3        VARCHAR (max)
			,variables   VARCHAR (2500)
		);

	DECLARE
	--PROCESS FLOW VARIABLES
		 @continue                BIT            = 1
		,@sqlScript               NVARCHAR(MAX)  = N''
	--LOGGING VARIABLES
		,@execObjectName          VARCHAR(256)   = 'dbo.sp_executeSQLinSequence'
		,@scriptCode              VARCHAR(25)    = ''
		,@status                  VARCHAR(50)    = ''
		,@logTreeLevel            TINYINT        = 0
		,@logSpaceTree            VARCHAR(5)     = '    '
		,@message                 VARCHAR(500)   = ''
		,@SQL                     VARCHAR(4000)  = ''
		,@variables               VARCHAR(2500)  = ''
	--FLAGS VARIABLES
		,@continue_C              BIT            = 1
	--GENERAL VARIABLES
		,@groupName_C             VARCHAR(50)    = ''
		,@SQLID_C                 INT            = 0
		,@destinationSchemaName_C NVARCHAR(128)  = N''
		,@destinationTableName_C  NVARCHAR(128)  = N''
		,@SQL1_C                  NVARCHAR(MAX)  = N''
		,@SQL2_C                  NVARCHAR(MAX)  = N''
		,@SQL3_C                  NVARCHAR(MAX)  = N''
		,@beginDateTime_C         DATETIME       = ''
		,@endDateTime_C           DATETIME       = ''
		,@timeElapsed_C           VARCHAR(50)    = '';
	
	--VARIABLES FOR LOGGING
		SET @variables = ' | @groupName = '         + ISNULL(CONVERT(VARCHAR(50),@groupName        ),'') + 
		                 ' | @SQLID = '             + ISNULL(CONVERT(VARCHAR(10),@SQLID            ),'') +
		                 ' | @executionID = '       + ISNULL(CONVERT(VARCHAR(20),@executionID      ),'') + 
		                 ' | @startLogTreeLevel = ' + ISNULL(CONVERT(VARCHAR(10),@startLogTreeLevel),'') + 
		                 ' | @debug = '             + ISNULL(CONVERT(VARCHAR(1) ,@debug            ),'') + 
		                 ' | @loggingType = '       + ISNULL(CONVERT(VARCHAR(1) ,@loggingType      ),'');
	
	--CHECKING THE EXECUTION ID PROVIDED AS AN INPUT PARAMETER
		IF(@executionID IS NULL OR @executionID = 0)
			BEGIN
				SET @executionID = NEXT VALUE FOR dbo.sq_BI_log_executionID;
			END
			
	----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
		SET @logTreeLevel = @startLogTreeLevel + 0;
		SET @scriptCode   = '';
		SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN Store Procedure';
		SET @status       = 'Information';
		SET @SQL          = '';
		IF(@loggingType IN (1,3))
			BEGIN
				INSERT INTO dbo.BI_log (executionID,sequenceID,logDateTime,object,scriptCode,status,message,SQL,variables)
				VALUES (@executionID,0,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
			END
		IF(@loggingType IN (2,3))
			RAISERROR(@message,10,1);
			
		SET @execObjectName = '';--This variable is set to BLANK because it's not necessary to set the same value in all the log records	
		SET @variables      = '';--This variable is set to BLANK because it's not necessary to set the same value in all the log records
	----------------------------------------------------- END INSERT LOG -----------------------------------------------------
	
	--VALIDATE INPUT PARAMETERS
		----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
			IF(@debug = 1)
				BEGIN
					SET @logTreeLevel = @startLogTreeLevel + 1;
					SET @scriptCode   = '';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN - Validating Input Parameters';
					SET @status       = 'Information';
					SET @SQL          = '';
					IF(@loggingType IN (1,3))
						BEGIN
							INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
							VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
						END
					IF(@loggingType IN (2,3))
						RAISERROR(@message,10,1);
				END
		----------------------------------------------------- END INSERT LOG -----------------------------------------------------
		
		IF(
			    @groupName != 'ALL'
			AND NOT EXISTS(
				SELECT 1
				FROM   dbo.BI_sqlToExecute a
				WHERE  a.groupName = @groupName
			)
		)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = @startLogTreeLevel + 2;
					SET @scriptCode   = 'COD-100E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The specified @GroupName (' + @groupName + ') does not exist at dbo.BI_sqlToExecute';
					SET @status       = 'ERROR';
					SET @SQL          = '';
					IF(@loggingType IN (1,3))
						BEGIN
							INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
							VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
						END
					IF(@loggingType IN (2,3))
						RAISERROR(@message,11,1);
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
			END
		ELSE IF(
			    @SQLID != 0
			AND NOT EXISTS(
				SELECT 1
				FROM   dbo.BI_sqlToExecute a
				WHERE  a.SQLID = @SQLID
			)
		)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = @startLogTreeLevel + 2;
					SET @scriptCode   = 'COD-200E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The specified @SQLID (' + CONVERT(VARCHAR(10),@SQLID) + ') does not exist at dbo.BI_sqlToExecute';
					SET @status       = 'ERROR';
					SET @SQL          = '';
					IF(@loggingType IN (1,3))
						BEGIN
							INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
							VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
						END
					IF(@loggingType IN (2,3))
						RAISERROR(@message,11,1);
				----------------------------------------------------- END INSERT LOG -----------------------------------------------------
			END
		----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
			IF(@debug = 1)
				BEGIN
					SET @logTreeLevel = @startLogTreeLevel + 1;
					SET @scriptCode   = '';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END - Validating Input Parameters';
					SET @status       = 'Information';
					SET @SQL          = '';
					IF(@loggingType IN (1,3))
						BEGIN
							INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
							VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
						END
					IF(@loggingType IN (2,3))
						RAISERROR(@message,10,1);
				END
		----------------------------------------------------- END INSERT LOG -----------------------------------------------------
	
	--GETTING THE INDEXES LIST TO GENERATE
		IF(@continue = 1)
			BEGIN
				--CREATING CURSOR WITH INDEXES TO CREATE
					IF (SELECT CURSOR_STATUS('LOCAL','ESQLIS_sql_cursor')) >= -1
						BEGIN
							DEALLOCATE ESQLIS_sql_cursor;
						END
			   	
				DECLARE ESQLIS_sql_cursor CURSOR LOCAL FOR
					SELECT
						 a.SQLID
						,a.groupName
						,COALESCE(a.destinationSchemaName,'')
						,COALESCE(a.destinationTableName,'')
						,COALESCE(a.SQL1,'')
						,COALESCE(a.SQL2,'')
						,COALESCE(a.SQL3,'')
					FROM 
						dbo.BI_sqlToExecute a (NOLOCK)
					WHERE
						a.disabled = 0
						AND (
							(
								    a.groupName = @groupName
								AND a.SQLID     = @SQLID
							)
							OR (
								    @groupName  = 'ALL'
								AND a.SQLID     = @SQLID
							)
							OR (
								    a.groupName = @groupName
								AND @SQLID      = 0
							)
							OR (
								    @groupName  = 'ALL'
								AND @SQLID      = 0
							)
						)
					ORDER BY
						 a.sequenceOrder ASC;
					   	
					OPEN ESQLIS_sql_cursor;
					
					FETCH NEXT FROM ESQLIS_sql_cursor 
					INTO @SQLID_C,@groupName_C,@destinationSchemaName_C,@destinationTableName_C,@SQL1_C,@SQL2_C,@SQL3_C;					
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = @startLogTreeLevel + 2;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN - ITERATION PROCESS';
								SET @status       = 'Information';
								SET @SQL          = '';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
					
					WHILE (@@FETCH_STATUS = 0)
						BEGIN
							SET @continue_C        = 1;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = @startLogTreeLevel + 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN - EXECUTING THE SQLID (' + CONVERT(VARCHAR(10),@SQLID_C) + ') | GROUPNAME (' + @groupName_C + ')';
										SET @status       = 'Information';
										SET @SQL          = '';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							
						--IF THE SQL TO EXECUTE MOVE DATA TO THE DESTINATION TABLE
							IF(
								@continue_C = 1 
								AND LEN(@destinationSchemaName_C) > 0
								AND LEN(@destinationTableName_C) > 0
							)
								BEGIN
								--VALIDATE SCHEMA
									IF(@continue_C = 1 AND SCHEMA_ID(@destinationSchemaName_C) IS NULL)
										BEGIN
											SET @continue_C = 0;
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												SET @logTreeLevel = @startLogTreeLevel + 4;
												SET @scriptCode   = 'COD-100E';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Schema (' + @destinationSchemaName_C + ') does not exist';
												SET @status       = 'Error';
												SET @SQL          = '';
												IF(@loggingType IN (1,3))
													BEGIN
														INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
														VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
													END
												IF(@loggingType IN (2,3))
													RAISERROR(@message,10,1);
											----------------------------------------------------- END INSERT LOG -----------------------------------------------------
										END
						
								--DROPPING DESTINATION TABLE
									IF(@continue_C = 1 AND OBJECT_ID(@destinationSchemaName_C + '.' + @destinationTableName_C) IS NOT NULL)
										BEGIN
											----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = @startLogTreeLevel + 4;
														SET @scriptCode   = '';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Table (' + @destinationSchemaName_C + '.' + @destinationTableName_C + ') Found. Proceeding to drop it';
														SET @status       = 'Information';
														SET @SQL          = '';
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
															END
														IF(@loggingType IN (2,3))
															RAISERROR(@message,10,1);
													END
											----------------------------------------------------- END INSERT LOG -----------------------------------------------------
											
											BEGIN TRY
												SET @sqlScript = 'DROP TABLE [' + @destinationSchemaName_C + '].[' + @destinationTableName_C + ']';
									
												----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
													IF(@debug = 1)
														BEGIN
															SET @logTreeLevel = @startLogTreeLevel + 4;
															SET @scriptCode   = 'COD-100I';
															SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
															SET @status       = 'Information';
															SET @SQL          = ISNULL(@sqlScript,'');
															IF(@loggingType IN (1,3))
																BEGIN
																	INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																	VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
																END
															IF(@loggingType IN (2,3))
																RAISERROR(@message,10,1);
														END
												----------------------------------------------------- END INSERT LOG -----------------------------------------------------
												
													EXEC(@sqlScript);
												
												----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
												IF(@debug = 1)
													BEGIN
														SET @logTreeLevel = @startLogTreeLevel + 4;
														SET @scriptCode   = '';
														SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Table (' + @destinationSchemaName_C + '.' + @destinationTableName_C + ') dropped';
														SET @status       = 'Information';
														SET @SQL          = '';
														IF(@loggingType IN (1,3))
															BEGIN
																INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
																VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
															END
														IF(@loggingType IN (2,3))
															RAISERROR(@message,10,1);
													END
												----------------------------------------------------- END INSERT LOG -----------------------------------------------------
											END TRY
											BEGIN CATCH
												SET @continue_C = 0;
												----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
													SET @logTreeLevel = @startLogTreeLevel + 4;
													SET @scriptCode   = 'COD-400E';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
													SET @status       = 'ERROR';
													SET @SQL          = ISNULL(@sqlScript,'');
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
														END
													IF(@loggingType IN (2,3))
														RAISERROR(@message,11,1);
												----------------------------------------------------- END INSERT LOG -----------------------------------------------------
											END CATCH
										END
								END
								
						--EXECUTING SQL
							IF(@continue_C = 1)
								BEGIN
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = @startLogTreeLevel + 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN - Executing the Script';
												SET @status       = 'Information';
												SET @SQL          = '';
												IF(@loggingType IN (1,3))
													BEGIN
														INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
														VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
													END
												IF(@loggingType IN (2,3))
													RAISERROR(@message,10,1);
											END
									----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									
									BEGIN TRY						
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = @startLogTreeLevel + 5;
													SET @scriptCode   = 'COD-100I';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Execute Script';
													SET @status       = 'Information';
													IF(@loggingType IN (1,3))
														BEGIN
															INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,SQL2,SQL3,variables)
															VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@SQL1_C,@SQL2_C,@SQL3_C,@variables)
														END
													IF(@loggingType IN (2,3))
														RAISERROR(@message,10,1);
												END
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
											SET @beginDateTime_C = GETDATE();
											
											EXEC(@SQL1_C + N' ' + @SQL2_C + N' ' + @SQL3_C);
											
											SET @endDateTime_C = GETDATE();
											
											SET @timeElapsed_C = CONVERT(VARCHAR(10),(CONVERT(INT,CONVERT(FLOAT,@endDateTime_C) - CONVERT(FLOAT,@beginDateTime_C)) * 24) + DATEPART(hh, @endDateTime_C - @beginDateTime_C)) + ':' + RIGHT('0' + CONVERT(VARCHAR(2),DATEPART(mi,@endDateTime_C - @beginDateTime_C)),2) + ':' + RIGHT('0' + CONVERT(VARCHAR(2),DATEPART(ss,@endDateTime_C - @beginDateTime_C)),2);
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = @startLogTreeLevel + 5;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL executed successfully. Time Elapsed: ' + @timeElapsed_C;
												SET @status       = 'Information';
												SET @SQL          = '';
												IF(@loggingType IN (1,3))
													BEGIN
														INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
														VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
													END
												IF(@loggingType IN (2,3))
													RAISERROR(@message,10,1);
											END
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									END TRY
									BEGIN CATCH
										SET @continue_C = 0;
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											SET @logTreeLevel = @startLogTreeLevel + 5;
											SET @scriptCode   = 'COD-400E';
											SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'SQL Error: Code(' + ISNULL(CONVERT(VARCHAR(20),ERROR_NUMBER()),'') + ') - '+ ISNULL(ERROR_MESSAGE(),'');
											SET @status       = 'ERROR';
											SET @SQL          = ISNULL(@sqlScript,'');
											IF(@loggingType IN (1,3))
												BEGIN
													INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
													VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
												END
											IF(@loggingType IN (2,3))
												RAISERROR(@message,11,1);
										----------------------------------------------------- END INSERT LOG -----------------------------------------------------
									END CATCH
									
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = @startLogTreeLevel + 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END - Executing the Script';
												SET @status       = 'Information';
												SET @SQL          = '';
												IF(@loggingType IN (1,3))
													BEGIN
														INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
														VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
													END
												IF(@loggingType IN (2,3))
													RAISERROR(@message,10,1);
											END
									----------------------------------------------------- END INSERT LOG -----------------------------------------------------
								END
							
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = @startLogTreeLevel + 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END - EXECUTING THE SQLID (' + CONVERT(VARCHAR(10),@SQLID_C) + ') | GROUPNAME (' + @groupName_C + ')';
										SET @status       = 'Information';
										SET @SQL          = '';
										IF(@loggingType IN (1,3))
											BEGIN
												INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
												VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
											END
										IF(@loggingType IN (2,3))
											RAISERROR(@message,10,1);
									END
							----------------------------------------------------- END INSERT LOG -----------------------------------------------------
							FETCH NEXT FROM ESQLIS_sql_cursor 
							INTO @SQLID_C,@groupName_C,@destinationSchemaName_C,@destinationTableName_C,@SQL1_C,@SQL2_C,@SQL3_C;
						END
						
						CLOSE ESQLIS_sql_cursor;
						
						IF (SELECT CURSOR_STATUS('LOCAL','ESQLIS_sql_cursor')) >= -1
							BEGIN
								DEALLOCATE ESQLIS_sql_cursor;
							END
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = @startLogTreeLevel + 2;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END - ITERATION PROCESS';
								SET @status       = 'Information';
								SET @SQL          = '';
								IF(@loggingType IN (1,3))
									BEGIN
										INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
										VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables)
									END
								IF(@loggingType IN (2,3))
									RAISERROR(@message,10,1);
							END
					----------------------------------------------------- END INSERT LOG -----------------------------------------------------
		   	END

	----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
		SET @logTreeLevel = @startLogTreeLevel + 0;
		SET @scriptCode   = '';
		SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END Stored Procedure';
		SET @status       = 'Information';
		SET @SQL          = '';
		IF(@loggingType IN (1,3))
			BEGIN
				INSERT INTO @BI_log (executionID,logDateTime,object,scriptCode,status,message,SQL,variables)
				VALUES (@executionID,GETDATE(),@execObjectName,@scriptCode,@status,@message,@sql,@variables);
			END
		IF(@loggingType IN (2,3))
			RAISERROR(@message,10,1);
	----------------------------------------------------- END INSERT LOG -----------------------------------------------------
		
	--Inserting Log into the physical table
		IF(@loggingType IN (1,3))
			BEGIN	
				INSERT INTO dbo.BI_log (
					 executionID
					,sequenceID
					,logDateTime
					,object 
					,scriptCode 
					,status 
					,message 
					,SQL
					,SQL2
					,SQL3 
					,variables 
				)
					SELECT * FROM @BI_log;
			END
			
	--RAISE ERROR IN CASE OF
		IF(@continue = 0)
			BEGIN
				DECLARE @errorMessage NVARCHAR(300);
				
				SET @errorMessage = N'PLEASE CHECK --> SELECT * FROM dbo.BI_log WHERE executionID = ' + CONVERT(NVARCHAR(20),@executionID);
				
				RAISERROR(@errorMessage,11,1);
			END 
END
GO
