CREATE PROCEDURE [dbo].[sp_createDGObjects]
	(
		@objectToCreate VARCHAR(128) = 'ALL'
	)
AS
/*
	Created by:    Mauricio Rivera
	Creation Date: 26 Jul 2018
*/
BEGIN
	DECLARE 
		 @sqlScript       NVARCHAR(MAX)
		,@DG_serverName   VARCHAR(128)
		,@DG_dataBaseName VARCHAR(128);
	
	SET @DG_serverName   = dbo.udf_getBIConfigParameter('DATAGOVERNOR-DETAILS',1);
	SET @DG_dataBaseName = dbo.udf_getBIConfigParameter('DATAGOVERNOR-DETAILS',2);
	
	IF(@objectToCreate = 'ALL' OR @objectToCreate = 'dbo.udf_validateDGJobDependants')
		BEGIN
			IF OBJECT_ID (N'dbo.udf_validateDGJobDependants') IS NOT NULL
				DROP FUNCTION dbo.udf_validateDGJobDependants
		
			SET @sqlScript = 'CREATE FUNCTION [dbo].[udf_validateDGJobDependants]
				(	
					@dg_dependantProcessGroupId VARCHAR(MAX)
				)
			RETURNS VARCHAR(1000) 
			AS
			BEGIN
				DECLARE @result VARCHAR(1000);
				
				IF(@dg_dependantProcessGroupId IS NULL OR LEN(@dg_dependantProcessGroupId) = 0)
					BEGIN
						SET @result = ''PROCEED'';
					END
				ELSE IF(
					EXISTS(
						SELECT 1
						FROM
							dbo.udf_DelimitedSplit8K(@dg_dependantProcessGroupId,'','') a INNER JOIN ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.vwProcessGroup b ON
								b.ProcessGroupID = a.Item
						WHERE b.ExecutionStatus = ''Executing''
					)
				)
					BEGIN
						SET @result = ''WAIT'';
					END
				ELSE IF(
					EXISTS(
						SELECT 1
						FROM
							dbo.udf_DelimitedSplit8K(@dg_dependantProcessGroupId,'','') a INNER JOIN ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.vwProcessGroup b ON
								b.ProcessGroupID = a.Item
						WHERE b.ExecutionStatus = ''Failure''
					)
				)
					BEGIN
						SET @result = ''ERROR - The Following Job(s) got an execution error (''
						+ STUFF(
							(
								SELECT ''| '' + b.ProcessGroupName
								FROM
									dbo.udf_DelimitedSplit8K(@dg_dependantProcessGroupId,'','') a INNER JOIN ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.vwProcessGroup b ON
										b.ProcessGroupID = a.Item
								WHERE b.ExecutionStatus = ''Failure''
								FOR XML PATH(''''), TYPE
							).value(''.'', ''VARCHAR(MAX)''), 1, 2, ''''
						) + '')'';
					END
				ELSE IF(
					EXISTS(
						SELECT 1
						FROM
							dbo.udf_DelimitedSplit8K(@dg_dependantProcessGroupId,'','') a INNER JOIN ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.vwProcessGroup b ON
								b.ProcessGroupID = a.Item
						WHERE b.ExecutionStatus IN (''Success'',''Never Executed'')
					)
				)
					BEGIN
						SET @result = ''PROCEED'';
					END
			
				RETURN @result
			END';
			
			EXEC(@sqlScript);
		END
	
	IF(@objectToCreate = 'ALL' OR @objectToCreate = 'dbo.vw_DGJobsByStatus')
		BEGIN
			IF OBJECT_ID (N'dbo.vw_DGJobsByStatus') IS NOT NULL
				DROP VIEW dbo.vw_DGJobsByStatus
			
			SET @sqlScript = 'CREATE VIEW dbo.vw_DGJobsByStatus AS
				SELECT
					 DG_group
					,aa.layer
					,aa.agentJobID
					,aa.jobName
					,aa.jobExecutionSequence
					,aa.okToProceed
					,aa.DependantProcessGroupID
					,aa.LastExecutionTime
					,aa.ExecutionStatus
				FROM
					(
						SELECT 
							 a.GroupName AS DG_group
							,CASE
								WHEN (a.ProjectName LIKE ''%Data Lake'') THEN ''DataLake''
								WHEN (a.ProjectName LIKE ''%Transformation'') THEN ''Transformation''
								WHEN (a.ProjectName LIKE ''%Rebuild Integration'') THEN ''RebuildIntegration''
								WHEN (a.ProjectName LIKE ''%Integration'') THEN ''Integration''
								WHEN (a.ProjectName LIKE ''%Tabular'') THEN ''Tabular''
								WHEN (a.ProjectName LIKE ''%Documentation'') THEN ''Documentation''
								WHEN (a.ProjectName LIKE ''%Data Governance'') THEN ''DataGovernance''
								ELSE a.ProjectName
							END AS layer
							,a.agentJobID
							,a.ProcessGroupName AS jobName
							,CASE
								WHEN(CHARINDEX(''-'',a.ProcessGroupName) > 0) THEN
									CASE 
										WHEN (ISNUMERIC(SUBSTRING(a.ProcessGroupName,1,CHARINDEX(''-'',a.ProcessGroupName) - 1)) = 1) THEN 
											CONVERT(INT,SUBSTRING(a.ProcessGroupName,1,CHARINDEX(''-'',a.ProcessGroupName) - 1))
										ELSE 0
									END
								ELSE 0
							END AS jobExecutionSequence
							,dbo.udf_validateDGJobDependants(REPLACE(a.DependantProcessGroupID,'' '','''')) AS okToProceed
							,a.DependantProcessGroupID
							,a.LastExecutionTime
							,CASE
								WHEN (a.ExecutionStatus = ''Executing''     ) THEN ''EXECUTING''
								WHEN (a.ExecutionStatus = ''Failure''       ) THEN ''FAILED''
								WHEN (a.ExecutionStatus = ''Success''       ) THEN ''SUCCEEDED''
								WHEN (a.ExecutionStatus = ''Never Executed'') THEN ''NEVER EXECUTED''
							END AS ExecutionStatus
						FROM
							' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.vwProcessGroup a  
					) aa';
			EXEC(@sqlScript);
		END

	IF(@objectToCreate = 'ALL' OR @objectToCreate = 'dbo.vw_DGobjectsByLayer')
		BEGIN
			IF OBJECT_ID (N'dbo.vw_DGobjectsByLayer') IS NOT NULL
				DROP VIEW dbo.vw_DGobjectsByLayer

			SET @sqlScript = 'CREATE VIEW dbo.vw_DGobjectsByLayer AS
				SELECT
					 aa.DG_group
					,aa.DG_project
					,aa.jobName
					,aa.agentJobId
					,aa.jobExecutionSequence
					,aa.taskId
					,aa.TaskName
					,aa.taskActive
					,OBJECT_ID(aa.SourceSchema + N''.'' + aa.sourceTableName) AS sourceObjectId
					,aa.SourceSchema
					,aa.sourceTableName
					,OBJECT_ID(aa.destinationSchema1 + N''.'' + aa.destinationTableName1) AS Destination1ObjectId
					,aa.destinationSchema1
					,aa.destinationTableName1
					,OBJECT_ID(aa.destinationSchema2 + N''.'' + aa.destinationTableName2) AS Destination2ObjectId
					,aa.destinationSchema2
					,aa.destinationTableName2
					,OBJECT_ID(aa.destinationSchema3 + N''.'' + aa.destinationTableName3) AS Destination3ObjectId
					,aa.destinationSchema3
					,aa.destinationTableName3
					,aa.finalTableIsCUR
				FROM
					(
						SELECT
							 a.DG_group
							,a.DG_project
							,a.jobName
							,a.agentJobId
							,a.jobExecutionSequence
							,a.taskId
							,a.TaskName
							,a.taskActive
							,COALESCE(a.paramSourceSchema,a.SourceSchema,'''') AS SourceSchema
							,COALESCE(a.paramSourceTable,a.sourceTableName,'''') AS sourceTableName
							,COALESCE(a.paramDestinationSchema1,a.destinationSchema,'''') AS destinationSchema1
							,COALESCE(a.paramDestinationTable1,a.destinationTableName,'''') AS destinationTableName1
							,COALESCE(a.paramDestinationSchema2,'''') AS destinationSchema2
							,COALESCE(a.paramDestinationTable2,'''') AS destinationTableName2
							,COALESCE(a.paramDestinationSchema3,'''') AS destinationSchema3
							,COALESCE(a.paramDestinationTable3,'''') AS destinationTableName3
							,COALESCE(a.finalTableIsCUR,'''') AS finalTableIsCUR
						FROM
							(
								SELECT 
									 grp.Description AS DG_group
									,pjt.Description AS DG_project
									,job.ProcessGroupName AS jobName
									,job.AgentJobID AS agentJobId
									,CASE
										WHEN(CHARINDEX(''-'',job.ProcessGroupName) > 0) THEN
											CASE 
												WHEN (ISNUMERIC(SUBSTRING(job.ProcessGroupName,1,CHARINDEX(''-'',job.ProcessGroupName) - 1)) = 1) THEN 
													CONVERT(INT,SUBSTRING(job.ProcessGroupName,1,CHARINDEX(''-'',job.ProcessGroupName) - 1))
												ELSE 0
											END
										ELSE 0
									END AS jobExecutionSequence
									,tsk.ProcessID AS taskId
									,tsk.ProcessName AS TaskName
									,jbtk.Active AS taskActive
									,tsk.ProcessTypeID
									,tskP.ProcessTypeName
									,CASE
										WHEN (tsk.ProcessTypeID = 3) THEN ''''
										ELSE tsk.SourceSchema 
									END AS SourceSchema
									,CASE
										WHEN (tsk.ProcessTypeID = 3) THEN ''''
										ELSE tbl.TableName 
									END AS sourceTableName
									,CASE
										WHEN (tsk.ProcessTypeID = 3) THEN ''''
										ELSE tsk.TargetSchema 
									END AS destinationSchema
									,CASE
										WHEN (tsk.ProcessTypeID = 3) THEN ''''
										ELSE tbl.TargetTableName 
									END AS destinationTableName
									,dbo.udf_getDGParameter(REPLACE(tbl.TargetTableName,''dbo.'',''''),tsk.ParameterNames,jbtk.ParameterValues,''sourceSchema'') AS paramSourceSchema
									,dbo.udf_getDGParameter(REPLACE(tbl.TargetTableName,''dbo.'',''''),tsk.ParameterNames,jbtk.ParameterValues,''sourceTable'') AS paramSourceTable
									,dbo.udf_getDGParameter(REPLACE(tbl.TargetTableName,''dbo.'',''''),tsk.ParameterNames,jbtk.ParameterValues,''destinationSchema1'') AS paramDestinationSchema1
									,dbo.udf_getDGParameter(REPLACE(tbl.TargetTableName,''dbo.'',''''),tsk.ParameterNames,jbtk.ParameterValues,''destinationTable1'') AS paramDestinationTable1
									,dbo.udf_getDGParameter(REPLACE(tbl.TargetTableName,''dbo.'',''''),tsk.ParameterNames,jbtk.ParameterValues,''destinationSchema2'') AS paramDestinationSchema2
									,dbo.udf_getDGParameter(REPLACE(tbl.TargetTableName,''dbo.'',''''),tsk.ParameterNames,jbtk.ParameterValues,''destinationTable2'') AS paramDestinationTable2
									,dbo.udf_getDGParameter(REPLACE(tbl.TargetTableName,''dbo.'',''''),tsk.ParameterNames,jbtk.ParameterValues,''destinationSchema3'') AS paramDestinationSchema3
									,dbo.udf_getDGParameter(REPLACE(tbl.TargetTableName,''dbo.'',''''),tsk.ParameterNames,jbtk.ParameterValues,''destinationTable3'') AS paramDestinationTable3
									,dbo.udf_getDGParameter(REPLACE(tbl.TargetTableName,''dbo.'',''''),tsk.ParameterNames,jbtk.ParameterValues,''finalTableIsCUR'') AS finalTableIsCUR
									,tsk.ParameterNames
									,jbtk.ParameterValues
								FROM
									' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.SecurityGroup grp INNER JOIN ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.SecurityProject pjt ON
										pjt.GroupID = grp.GroupID
									INNER JOIN ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.ProcessGroup job ON
										job.ProjectID = pjt.ProjectID
									INNER JOIN ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.ProcessGroupSequence jbtk ON
										jbtk.ProcessGroupID = job.ProcessGroupID
									INNER JOIN ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.Process tsk ON
										tsk.ProcessID = jbtk.ProcessID
									INNER JOIN ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.ProcessType tskP ON
										tskP.ProcessTypeID = tsk.ProcessTypeID
									LEFT JOIN ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.ProcessTable tbl ON
										tbl.ProcessID = tsk.ProcessID
								WHERE
									tsk.ProcessTypeID IN (2,3)
							) a
						) aa';
			EXEC(@sqlScript);
		END
		
	IF(@objectToCreate = 'ALL' OR @objectToCreate = 'dbo.vw_agentJobStatus')
		BEGIN
			IF OBJECT_ID (N'dbo.vw_agentJobStatus') IS NOT NULL
				DROP VIEW dbo.vw_agentJobStatus

			SET @sqlScript = 'CREATE VIEW dbo.vw_agentJobStatus 
				AS
					SELECT
						 aa.jobId
						,aa.jobName
						,CASE
							WHEN (aa.currentStatus = -1) THEN ''EXECUTING''
							WHEN (aa.currentStatus =  0) THEN ''FAILED''
							WHEN (aa.currentStatus =  1) THEN ''SUCCEEDED''
							WHEN (aa.currentStatus =  2) THEN ''RETRY''
							WHEN (aa.currentStatus =  3) THEN ''CANCELED''
							WHEN (aa.currentStatus =  5) THEN ''UNKNOWN''
						END AS status
					FROM
						(
							SELECT
								 a.job_id AS jobId
								,c.name AS jobName
								,CASE
									WHEN (a.run_Requested_date IS NOT NULL AND a.stop_execution_date IS NULL) THEN -1
									ELSE b.last_run_outcome	
								END AS currentStatus
							FROM 
								' + @DG_serverName + '.msdb.dbo.sysjobactivity a INNER JOIN ' + @DG_serverName + '.msdb.dbo.sysjobsteps b ON
									b.job_id = a.job_id
								INNER JOIN ' + @DG_serverName + '.msdb.dbo.sysjobs c ON
									c.job_id = a.job_id
							WHERE 
								a.run_Requested_date = (
									SELECT MAX(aa.run_Requested_date) 
									FROM ' + @DG_serverName + '.msdb.dbo.sysjobactivity aa
									WHERE aa.job_id = a.job_id
								)
						) aa';
			EXEC(@sqlScript);
		END
	
	IF(@objectToCreate = 'ALL' OR @objectToCreate = 'dbo.vw_DGLastExecutionIdByJobId')
		BEGIN
			IF OBJECT_ID (N'dbo.vw_DGLastExecutionIdByJobId') IS NOT NULL
				DROP VIEW dbo.vw_DGLastExecutionIdByJobId

			SET @sqlScript = 'CREATE VIEW dbo.vw_DGLastExecutionIdByJobId 
				AS
					SELECT
						 DISTINCT 
						 a.AgentJobID
						,c.ProcessExecutionID
						,b.ProcessGroupExecutionID
					FROM
						' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.ProcessGroup a INNER JOIN ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.ProcessGroupExecution b ON
							b.ProcessGroupID = a.ProcessGroupID
						INNER JOIN ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.ProcessExecution c ON
							    c.Status                  = ''Running''
							AND c.ProcessGroupExecutionID = b.ProcessGroupExecutionID
					WHERE
						c.StartTime = (
							SELECT MAX(aa.StartTime)
							FROM ' + @DG_serverName + '.' + @DG_dataBaseName + '.dbo.ProcessExecution aa
							WHERE aa.ProcessGroupExecutionID = b.ProcessGroupExecutionID
						)';
			EXEC(@sqlScript);
		END
END
GO
