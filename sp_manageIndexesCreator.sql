IF OBJECT_ID (N'dbo.BI_indexes') IS NOT NULL
	DROP TABLE dbo.BI_indexes
GO

CREATE TABLE dbo.BI_indexes
	(
	indexID                    INT IDENTITY NOT NULL,
	groupName                  VARCHAR (50) NOT NULL,
	schemaName                 NVARCHAR (128) NOT NULL,
	tableName                  NVARCHAR (128) NOT NULL,
	indexType                  NVARCHAR (20) DEFAULT ('NONCLUSTERED') NOT NULL,
	columnsName                NVARCHAR (1000) NOT NULL,
	columnsSort                NVARCHAR (200) NOT NULL,
	includesName               NVARCHAR (1000) DEFAULT ('') NOT NULL,
	includesSort               NVARCHAR (200) DEFAULT ('') NOT NULL,
	forceToRecreate            BIT DEFAULT ((0)) NOT NULL,
	padIndex                   BIT DEFAULT ((0)) NOT NULL,
	statisticsNoReCompute      BIT DEFAULT ((0)) NOT NULL,
	sortInTempdb               BIT DEFAULT ((0)) NOT NULL,
	dropExisting               BIT DEFAULT ((0)) NOT NULL,
	online                     BIT DEFAULT ((0)) NOT NULL,
	allowRowLocks              BIT DEFAULT ((1)) NOT NULL,
	allowPageLocks             BIT DEFAULT ((1)) NOT NULL,
	onPartition                NVARCHAR (128) DEFAULT ('PRIMARY') NOT NULL,
	lastUpdateDate             DATETIME DEFAULT (getdate()) NOT NULL,
	elapsedCreationTimeSeconds INT DEFAULT ((0)) NOT NULL,
	CONSTRAINT PK_BI_indexes PRIMARY KEY (indexID)
	)
GO

IF OBJECT_ID (N'dbo.sp_manageIndexesCreator') IS NOT NULL
	DROP PROCEDURE dbo.sp_manageIndexesCreator
GO

CREATE PROCEDURE [dbo].[sp_manageIndexesCreator] 
(
	 @groupName         VARCHAR(50)  = 'ALL'
	,@schemaName        VARCHAR(128) = 'ALL'
	,@tableName         VARCHAR(128) = 'ALL'
	,@executionID       BIGINT       = 0
	,@startLogTreeLevel TINYINT      = 0
	,@debug             BIT          = 0
	,@loggingType       BIT          = 1 --1) Table | 2) DataGovernor | 3) Table & DataGovernor
)
/*
	Developed by: Mauricio Rivera Senior
	Date: 5 Sep 2018
	
	MODIFICATIONS
		
		
	LAST USED LOGGING IDS:
		- ERROR       (COD-500E)
		- WARNING     (COD-400W)
		- INFORMATION (COD-200I)
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
				SQL         VARCHAR (MAX) NOT NULL,
				variables   VARCHAR (2500) NOT NULL,
			);
			CREATE CLUSTERED INDEX CIX_dbo_BIlog_1 ON dbo.BI_log (executionID);
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
			,SQL         VARCHAR (MAX)
			,variables   VARCHAR (2500)
		);
	
	--Declare Table variable to alocate the list of indexes to create
		DECLARE @IndexesList TABLE (
			 indexID               INT 
			,schemaName            NVARCHAR(128)
			,tableName             NVARCHAR(128)
			,indexType             NVARCHAR(20)
			,columnsName           NVARCHAR(1000)
			,ColumnsSort           NVARCHAR(200)
			,includesName          NVARCHAR(1000)
			,includesSort          NVARCHAR(200)
			,padIndex              BIT
			,statisticsNoReCompute BIT
			,sortInTempdb          BIT
			,dropExisting          BIT
			,online                BIT
			,allowRowLocks         BIT
			,allowPageLocks        BIT
			,onPartition           NVARCHAR(128)
		);

	DECLARE
	--PROCESS FLOW VARIABLES
		 @continue                BIT            = 1
		,@sqlScript               NVARCHAR(MAX)  = N''
	--LOGGING VARIABLES
		,@execObjectName          VARCHAR(256)   = 'dbo.sp_manageIndexesCreator'
		,@scriptCode              VARCHAR(25)    = ''
		,@status                  VARCHAR(50)    = ''
		,@logTreeLevel            TINYINT        = 0
		,@logSpaceTree            VARCHAR(5)     = '    '
		,@message                 VARCHAR(500)   = ''
		,@SQL                     VARCHAR(4000)  = ''
		,@variables               VARCHAR(2500)  = ''
	--FLAGS VARIABLES
	--GENERAL VARIABLES
		,@varchar                 VARCHAR(1000)  = ''
		,@int                     INT            = 0
		,@indexNameLength         TINYINT        = 120
		,@indexName               NVARCHAR(128)  = N''
		,@columnsNameSorted       NVARCHAR(1000) = N''
		,@indexBeginExecution     DATETIME       = ''
		,@indexEndExecution       DATETIME       = ''
		,@indexTimeElapsed        VARCHAR(10)    = ''
		,@continue_C              BIT            = 1
		,@indexID_C               INT = 0
		,@object_id_C             INT = 0
		,@schemaName_C            NVARCHAR(128)  = N''
		,@tableName_C             NVARCHAR(128)  = N''
		,@indexType_C             NVARCHAR(20)   = N''
		,@columnsName_C           NVARCHAR(1000) = N''
		,@ColumnsSort_C           NVARCHAR(200)  = N''
		,@includesName_C          NVARCHAR(1000) = N''
		,@padIndex_C              NVARCHAR(3)    = N'OFF'
		,@statisticsNoReCompute_C NVARCHAR(3)    = N'OFF'
		,@sortInTempdb_C          NVARCHAR(3)    = N'OFF'
		,@dropExisting_C          NVARCHAR(3)    = N'OFF'
		,@online_C                NVARCHAR(3)    = N'OFF'
		,@allowRowLocks_C         NVARCHAR(3)    = N'ON'
		,@allowPageLocks_C        NVARCHAR(3)    = N'ON'
		,@onPartition_C           NVARCHAR(128)  = N''
		,@forceToRecreate_C       BIT            = 0;
	
	--VARIABLES FOR LOGGING
		SET @variables = ' | @groupName = '         + ISNULL(CONVERT(VARCHAR(128) ,@groupName        ),'') + 
		                 ' | @schemaName = '        + ISNULL(CONVERT(VARCHAR(128) ,@schemaName       ),'') +
		                 ' | @tableName = '         + ISNULL(CONVERT(VARCHAR(128) ,@tableName        ),'') + 
		                 ' | @executionID = '       + ISNULL(CONVERT(VARCHAR(20)  ,@executionID      ),'') + 
		                 ' | @startLogTreeLevel = ' + ISNULL(CONVERT(VARCHAR(10)  ,@startLogTreeLevel),'') + 
		                 ' | @debug = '             + ISNULL(CONVERT(VARCHAR(1)   ,@debug            ),'') + 
		                 ' | @loggingType = '       + ISNULL(CONVERT(VARCHAR(1)   ,@loggingType      ),'');
	
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
				FROM   dbo.BI_indexes a
				WHERE  a.groupName = @groupName
			)
		)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = @startLogTreeLevel + 2;
					SET @scriptCode   = 'COD-100E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The specified @GroupName (' + @groupName + ') does not exist at dbo.BI_indexes';
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
			    @schemaName != 'ALL'
			AND SCHEMA_ID(@schemaName) IS NULL
		)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = @startLogTreeLevel + 2;
					SET @scriptCode   = 'COD-200E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The specified @schemaName (' + @schemaName + ') is not valid';
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
			    @tableName != 'ALL'
			AND NOT EXISTS(
				SELECT 1
				FROM   sys.objects a
				WHERE  
					    a.name        = @tableName
					AND a.[schema_id] = SCHEMA_ID(@schemaName)
					AND a.type        IN ('U')
			)
		)
			BEGIN
				SET @continue = 0;
				----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
					SET @logTreeLevel = @startLogTreeLevel + 2;
					SET @scriptCode   = 'COD-300E';
					SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The specified @tableName (' + @tableName + ') does not exist';
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
					IF (SELECT CURSOR_STATUS('LOCAL','MIC_indexes_cursor')) >= -1
						BEGIN
							DEALLOCATE MIC_indexes_cursor;
						END
		
				DECLARE MIC_indexes_cursor CURSOR LOCAL FOR
					SELECT
						 indexID
						,CONVERT(INT,OBJECT_ID(schemaName + N'.' + tableName)) AS object_id
						,schemaName
						,tableName
						,indexType
						,columnsName
						,ColumnsSort
						,includesName
						,CONVERT(VARCHAR(5),
							CASE
								WHEN (padIndex = 0) THEN 'OFF'
								ELSE 'ON'
							END 
						) AS padIndex
						,CONVERT(VARCHAR(5),
							CASE
								WHEN (statisticsNoReCompute = 0) THEN 'OFF'
								ELSE 'ON'
							END 
						) AS statisticsNoReCompute
						,CONVERT(VARCHAR(5),
							CASE
								WHEN (sortInTempdb = 0) THEN 'OFF'
								ELSE 'ON'
							END 
						) AS sortInTempdb
						,CONVERT(VARCHAR(5),
							CASE
								WHEN (dropExisting = 0) THEN 'OFF'
								ELSE 'ON'
							END 
						) AS dropExisting
						,CONVERT(VARCHAR(5),
							CASE
								WHEN (online = 0) THEN 'OFF'
								ELSE 'ON'
							END 
						) AS online
						,CONVERT(VARCHAR(5),
							CASE
								WHEN (allowRowLocks = 0) THEN 'OFF'
								ELSE 'ON'
							END 
						) AS allowRowLocks
						,CONVERT(VARCHAR(5),
							CASE
								WHEN (allowPageLocks = 0) THEN 'OFF'
								ELSE 'ON'
							END 
						) AS allowPageLocks
						,onPartition
						,forceToRecreate
					FROM 
						dbo.BI_indexes a (NOLOCK)
					WHERE
						(
							    a.groupName  = @groupName
							AND a.schemaName = @schemaName
							AND a.tableName  = @tableName
						)
						OR (
							    @groupName   = 'ALL'
							AND a.schemaName = @schemaName
							AND a.tableName  = @tableName
						)
						OR (
							    a.groupName = @groupName
							AND @schemaName = 'ALL'
							AND a.tableName = @tableName
						)
						OR (
							    a.groupName  = @groupName
							AND a.schemaName = @schemaName
							AND @tableName   = 'ALL'
						)
						OR (
							    a.groupName = @groupName
							AND @schemaName = 'ALL'
							AND @tableName  = 'ALL'
						)
						OR (
							    @groupName   = 'ALL'
							AND a.schemaName = @schemaName
							AND @tableName   = 'ALL'
						)
						OR (
							    @groupName  = 'ALL'
							AND @schemaName = 'ALL'
							AND a.tableName = @tableName
						)
						OR (
							    @groupName  = 'ALL'
							AND @schemaName = 'ALL'
							AND @tableName  = 'ALL'
						)
					ORDER BY
						 schemaName  ASC
						,tableName   ASC
						,indexType   ASC 
						,columnsName ASC;
					   	
					OPEN MIC_indexes_cursor;
					
					FETCH NEXT FROM MIC_indexes_cursor 
					INTO @indexID_C,@object_id_C,@schemaName_C,@tableName_C,@indexType_C,@columnsName_C,@ColumnsSort_C,@includesName_C,@padIndex_C,@statisticsNoReCompute_C,@sortInTempdb_C,@dropExisting_C,@online_C,@allowRowLocks_C,@allowPageLocks_C,@onPartition_C,@forceToRecreate_C;					
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = @startLogTreeLevel + 2;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN - Creating Indexex';
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
							SET @indexName         = N'';
							SET @columnsNameSorted = N'';
							SET @varchar           = '';
							SET @int               = 0;
							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = @startLogTreeLevel + 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN - Creating ' + @indexType_C + ' Index on (' + @schemaName_C + '.' + @tableName_C + ') Over Columns (' + @columnsName_C + ')';
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
							
						--VALIDATE SCHEMA
							IF(@continue_C = 1 AND SCHEMA_ID(@schemaName_C) IS NULL)
								BEGIN
									SET @continue_C = 0;
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										SET @logTreeLevel = @startLogTreeLevel + 4;
										SET @scriptCode   = 'COD-100W';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Schema (' + @schemaName_C + ') does not exist';
										SET @status       = 'Warning';
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
						
						--VALIDATE TABLE
							IF(@continue_C = 1 AND @object_id_C IS NULL)
								BEGIN
									SET @continue_C = 0;
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										SET @logTreeLevel = @startLogTreeLevel + 4;
										SET @scriptCode   = 'COD-200W';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Table (' + @tableName_C + ') does not exist in the Schema (' + @schemaName_C + ')';
										SET @status       = 'Warning';
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
								
						--VALIDATE COLUMNS ON THE TABLE
							IF(
								@continue_C = 1 
								AND EXISTS(
									SELECT 1
									FROM 
										dbo.udf_DelimitedSplit8K(@columnsName_C,',') a LEFT JOIN sys.columns b ON
											    b.object_id = @object_id_C
											AND b.name = a.Item
									WHERE
										b.name IS NULL
								)
							)
								BEGIN
									SET @continue_C = 0;
									SET @varchar = (
										SELECT
											STUFF(
												(
													SELECT ',' + a.Item
													FROM 
														dbo.udf_DelimitedSplit8K(@columnsName_C,',') a LEFT JOIN sys.columns b ON
															    b.object_id = @object_id_C
															AND b.name = a.Item
													WHERE
														b.name IS NULL
													ORDER BY
														a.ItemNumber ASC
													FOR XML PATH(''), TYPE
												).value('.', 'VARCHAR(MAX)'), 1, 1, ''
											)
									);
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										SET @logTreeLevel = @startLogTreeLevel + 4;
										SET @scriptCode   = 'COD-300W';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Columns (' + @varchar + ') does not exist in the Table (' + @schemaName_C + '.' + @tableName_C + ')';
										SET @status       = 'Warning';
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
							
						--VALIDATE INCLUDE COLUMNS ON THE TABLE
							IF(
								@continue_C = 1 
								AND LEN(@includesName_C) > 0
								AND EXISTS(
									SELECT 1
									FROM 
										dbo.udf_DelimitedSplit8K(@includesName_C,',') a LEFT JOIN sys.columns b ON
											    b.object_id = @object_id_C
											AND b.name = a.Item
									WHERE
										b.name IS NULL
								)
							)
								BEGIN
									SET @continue_C = 0;
									
									SET @varchar = (
										SELECT
											STUFF(
												(
													SELECT ',' + a.Item
													FROM 
														dbo.udf_DelimitedSplit8K(@includesName_C,',') a LEFT JOIN sys.columns b ON
															    b.object_id = @object_id_C
															AND b.name = a.Item
													WHERE
														b.name IS NULL
													ORDER BY
														a.ItemNumber ASC
													FOR XML PATH(''), TYPE
												).value('.', 'VARCHAR(MAX)'), 1, 1, ''
											)
									);
									
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										SET @logTreeLevel = @startLogTreeLevel + 4;
										SET @scriptCode   = 'COD-400W';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'The Columns (' + @varchar + ') used as Include, does not exist in the Table (' + @schemaName_C + '.' + @tableName_C + ')';
										SET @status       = 'Warning';
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
						
						--VALIDATE IF INDEX EXIST
							IF(
								@continue_C = 1 
								AND @forceToRecreate_C = 0
								AND EXISTS(
									SELECT 1
									FROM
										(
											SELECT
												COALESCE(
													STUFF(
														(
															SELECT
																',' + xbb.name
															FROM
																sys.index_columns xaa INNER JOIN sys.columns xbb ON
																	    xbb.object_id = xaa.object_id
																	AND xbb.column_id = xaa.column_id
															WHERE
																    xaa.object_id          = xa.object_id
																AND xaa.index_id           = xa.index_id 
																AND xaa.is_included_column = 0
															ORDER BY
																xaa.index_column_id ASC
															FOR XML PATH(''), TYPE
														).value('.', 'VARCHAR(MAX)'), 1, 1, ''
													) ,''
												)AS columnsName
												,COALESCE(
													STUFF(
														(
															SELECT
																',' + xbb.name
															FROM
																sys.index_columns xaa INNER JOIN sys.columns xbb ON
																	    xbb.object_id = xaa.object_id
																	AND xbb.column_id = xaa.column_id
															WHERE
																    xaa.object_id          = xa.object_id
																AND xaa.index_id           = xa.index_id 
																AND xaa.is_included_column = 1
															ORDER BY
																xaa.index_column_id ASC
															FOR XML PATH(''), TYPE
														).value('.', 'VARCHAR(MAX)'), 1, 1, ''
													),''
												) AS includesName
											FROM
												sys.indexes xa
											WHERE
												    xa.object_id = @object_id_C
												AND xa.name      IS NOT NULL
										) a
									WHERE
										    a.columnsName  = @columnsName_C
										AND a.includesName = @includesName_C
								)
							)
								BEGIN
									SET @continue_C = 0;
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = @startLogTreeLevel + 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Index Found';
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
								
						--IN CASE OF @forceToRecreate_C IS (1), PROCEED TO DROP THE INDEX
							IF(
								@continue_C = 1
								AND @forceToRecreate_C = 1
								AND EXISTS(
									SELECT 1
									FROM
										(
											SELECT
												COALESCE(
													STUFF(
														(
															SELECT
																',' + xbb.name
															FROM
																sys.index_columns xaa INNER JOIN sys.columns xbb ON
																	    xbb.object_id = xaa.object_id
																	AND xbb.column_id = xaa.column_id
															WHERE
																    xaa.object_id          = xa.object_id
																AND xaa.index_id           = xa.index_id 
																AND xaa.is_included_column = 0
															ORDER BY
																xaa.index_column_id ASC
															FOR XML PATH(''), TYPE
														).value('.', 'VARCHAR(MAX)'), 1, 1, ''
													) ,''
												)AS columnsName
												,COALESCE(
													STUFF(
														(
															SELECT
																',' + xbb.name
															FROM
																sys.index_columns xaa INNER JOIN sys.columns xbb ON
																	    xbb.object_id = xaa.object_id
																	AND xbb.column_id = xaa.column_id
															WHERE
																    xaa.object_id          = xa.object_id
																AND xaa.index_id           = xa.index_id 
																AND xaa.is_included_column = 1
															ORDER BY
																xaa.index_column_id ASC
															FOR XML PATH(''), TYPE
														).value('.', 'VARCHAR(MAX)'), 1, 1, ''
													),''
												) AS includesName
											FROM
												sys.indexes xa
											WHERE
												    xa.object_id = @object_id_C
												AND xa.name      IS NOT NULL
										) a
									WHERE
										    a.columnsName  = @columnsName_C
										AND a.includesName = @includesName_C
								)
							)
								BEGIN
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = @startLogTreeLevel + 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Index Found and Force to Recreate it is ON';
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
									
									----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = @startLogTreeLevel + 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN - Drop Index';
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
									
									SET @varchar = (
										SELECT a.indexName
										FROM
											(
												SELECT
													 xa.name AS indexName
													,COALESCE(
														STUFF(
															(
																SELECT
																	',' + xbb.name
																FROM
																	sys.index_columns xaa INNER JOIN sys.columns xbb ON
																		    xbb.object_id = xaa.object_id
																		AND xbb.column_id = xaa.column_id
																WHERE
																	    xaa.object_id          = xa.object_id
																	AND xaa.index_id           = xa.index_id 
																	AND xaa.is_included_column = 0
																ORDER BY
																	xaa.index_column_id ASC
																FOR XML PATH(''), TYPE
															).value('.', 'VARCHAR(MAX)'), 1, 1, ''
														) ,''
													)AS columnsName
													,COALESCE(
														STUFF(
															(
																SELECT
																	',' + xbb.name
																FROM
																	sys.index_columns xaa INNER JOIN sys.columns xbb ON
																		    xbb.object_id = xaa.object_id
																		AND xbb.column_id = xaa.column_id
																WHERE
																	    xaa.object_id          = xa.object_id
																	AND xaa.index_id           = xa.index_id 
																	AND xaa.is_included_column = 1
																ORDER BY
																	xaa.index_column_id ASC
																FOR XML PATH(''), TYPE
															).value('.', 'VARCHAR(MAX)'), 1, 1, ''
														),''
													) AS includesName
												FROM
													sys.indexes xa
												WHERE
													    xa.object_id = @object_id_C
													AND xa.name      IS NOT NULL
											) a
										WHERE
											    a.columnsName  = @columnsName_C
											AND a.includesName = @includesName_C
									);
									
									BEGIN TRY
										SET @sqlScript = 'DROP INDEX [' + @varchar + '] ON ' + @schemaName_C + '.' + @tableName_C;
									
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = @startLogTreeLevel + 5;
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
										
										---------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = @startLogTreeLevel + 5;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Index Dropped successfully';
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
									
									---------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = @startLogTreeLevel + 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END - Drop Index';
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
						
						--GETTING INDEX NAME
							IF(@continue_C = 1)
								BEGIN
									---------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = @startLogTreeLevel + 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN - Getting Index Name';
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
									
										SET @indexName = (
											SELECT
												CASE
													WHEN (@indexType_C = 'NONCLUSTERED') THEN N'NCI_'
													WHEN (@indexType_C = 'CLUSTERED'   ) THEN N'CI_'
													WHEN (@indexType_C = 'UNIQUE'      ) THEN N'UI_'
													ELSE                                      N'XI_'
												END + @columnsName_C
										)
										
										IF(LEN(@indexName) > @indexNameLength)
											BEGIN
												SET @indexName = (
													SELECT 
														CASE
															WHEN (RIGHT(SUBSTRING(@indexName,1,@indexNameLength),1) IN ('_',',')) THEN
																SUBSTRING(@indexName,1,@indexNameLength - 1)
															ELSE 
																SUBSTRING(@indexName,1,@indexNameLength)
														END
												);
	
												SET @int = (
													SELECT COUNT(a.name) + 1
													FROM sys.indexes a
													WHERE
														    a.object_id    = @object_id_C
														AND a.name      LIKE @indexName + '%'
												);
												
												SET @indexName = @indexName + N'_' + CONVERT(VARCHAR(10),@int);
											END
									
										---------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = @startLogTreeLevel + 4;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Index Name assigned (' + @indexName + ')';
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
									
									---------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = @startLogTreeLevel + 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END - Getting Index Name';
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
						
						--ASSIGNING THE SORT TO THE INDEX COLUMNS
							IF(@continue_C = 1)
								BEGIN
									---------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = @startLogTreeLevel + 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN - Assigning the Sort to the Index Columns';
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
									
										SET @columnsNameSorted = (
											SELECT 
												STUFF(
													(
														SELECT
															N',[' + xa.Item + N'] ' + xb.Item
														FROM
															dbo.udf_DelimitedSplit8K(@columnsName_C,',') xa LEFT JOIN dbo.udf_DelimitedSplit8K(@ColumnsSort_C,',') xb ON
																xb.ItemNumber = xa.ItemNumber
														ORDER BY 
															xa.ItemNumber ASC 
														FOR XML PATH(''), TYPE
													).value('.', 'VARCHAR(MAX)'), 1, 1, ''
												)
										);
									
										---------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = @startLogTreeLevel + 4;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Columns Name Sort Assigned (' + @columnsNameSorted + ')';
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
									
									---------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = @startLogTreeLevel + 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END - Assigning the Sort to the Index Columns';
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
						
						--CREATING INDEX
							IF(@continue_C = 1)
								BEGIN
									---------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = @startLogTreeLevel + 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN - Creating Index';
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
										SET @sqlScript = N'CREATE ' + @indexType_C + N' INDEX [' + @indexName +  N'] ON [' + @schemaName_C + N'].[' + @tableName_C + N'] (' + @columnsNameSorted + N')';
										
										IF(LEN(@includesName_C) > 0)
											BEGIN
												SET @sqlScript = @sqlScript + N' INCLUDE (' + @includesName_C + ')';
											END
										
										SET @sqlScript = @sqlScript + N' WITH (PAD_INDEX = ' + @padIndex_C + ', STATISTICS_NORECOMPUTE = ' + @statisticsNoReCompute_C + ', SORT_IN_TEMPDB = ' + @sortInTempdb_C + ', DROP_EXISTING = ' + @dropExisting_C + ', ONLINE = ' + @online_C + ', ALLOW_ROW_LOCKS = ' + @allowRowLocks_C + ', ALLOW_PAGE_LOCKS = ' + @allowPageLocks_C + ') ON [' + @onPartition_C + N']';
										
										----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = @startLogTreeLevel + 5;
													SET @scriptCode   = 'COD-200I';
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
										
										SET @indexBeginExecution = GETDATE();
										EXEC(@sqlScript);
										SET @indexEndExecution = GETDATE();
										
										SET @indexTimeElapsed = CONVERT(VARCHAR(10),(CONVERT(INT,CONVERT(FLOAT,@indexEndExecution) - CONVERT(FLOAT,@indexBeginExecution)) * 24) + DATEPART(hh, @indexEndExecution - @indexBeginExecution)) + ':' + RIGHT('0' + CONVERT(VARCHAR(2),DATEPART(mi,@indexEndExecution - @indexBeginExecution)),2) + ':' + RIGHT('0' + CONVERT(VARCHAR(2),DATEPART(ss,@indexEndExecution - @indexBeginExecution)),2)
										
										---------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
											IF(@debug = 1)
												BEGIN
													SET @logTreeLevel = @startLogTreeLevel + 5;
													SET @scriptCode   = '';
													SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'Index Created Successfully. Time Elapsed (' + @indexTimeElapsed + ')';
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
											SET @scriptCode   = 'COD-500E';
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
									---------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
										IF(@debug = 1)
											BEGIN
												SET @logTreeLevel = @startLogTreeLevel + 4;
												SET @scriptCode   = '';
												SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END - Creating Index';
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
							
							--UPDATING INDEX STATS
								IF(@continue_C = 1)
									BEGIN
										UPDATE 
											dbo.BI_indexes
										SET 
											 lastUpdateDate = @indexEndExecution
											,elapsedCreationTimeSeconds = DATEDIFF(second,@indexBeginExecution,@indexEndExecution)
											,forceToRecreate = 0
										WHERE
											indexID = @indexID_C;
									END

							----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
								IF(@debug = 1)
									BEGIN
										SET @logTreeLevel = @startLogTreeLevel + 3;
										SET @scriptCode   = '';
										SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'END - Creating ' + @indexType_C + ' Index on (' + @schemaName_C + '.' + @tableName_C + ') Over Columns (' + @columnsName_C + ')';
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
							
							FETCH NEXT FROM MIC_indexes_cursor 
							INTO @indexID_C,@object_id_C,@schemaName_C,@tableName_C,@indexType_C,@columnsName_C,@ColumnsSort_C,@includesName_C,@padIndex_C,@statisticsNoReCompute_C,@sortInTempdb_C,@dropExisting_C,@online_C,@allowRowLocks_C,@allowPageLocks_C,@onPartition_C,@forceToRecreate_C;	
						END
					
					----------------------------------------------------- BEGIN INSERT LOG -----------------------------------------------------
						IF(@debug = 1)
							BEGIN
								SET @logTreeLevel = @startLogTreeLevel + 2;
								SET @scriptCode   = '';
								SET @message      = REPLICATE(@logSpaceTree,@logTreeLevel) + 'BEGIN - Creating Indexex';
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

