CREATE PROCEDURE dbo.sp_manageAgentJobs
	(
		 @jobName NVARCHAR(128) = ''
		,@action  NVARCHAR(30)  = '' --[run,stop,status]
	)
AS
/*
  Developed by: Mauricio Rivera Senior
  Description: This SP manage the agent jobs. You can run, stop or get the status of a Job by the name of it. 
*/
BEGIN
	DECLARE 
		 @continue SMALLINT
		,@msg      NVARCHAR(100)
		,@jobID    NVARCHAR(128);
		
	SET @continue = 1;
	SET @msg      = '';
	SET @jobID    = '';
	
	--CHECKING INPUT VARIABLES
		IF(@action NOT IN (N'run',N'stop',N'status'))
			BEGIN
				SET @continue = 0;
				SET @msg      = N'The input variable action only accepts run, stop or status as its values';
			END
	
	--CHECKING THE EXISTENCE OF THE JOB
		IF(@continue = 1)
			BEGIN
				--GETTING THE JOBID FROM THE NAME OF THE INPUT PARAMETER jobName
					SELECT @jobID = a.job_id 
					FROM [msdb].[dbo].[sysjobs] a 
					WHERE a.name = @jobName;
				
				--IF JOB_ID IS BLANK THE JOB DOES NOT EXISTS
					IF(RTRIM(LTRIM(ISNULL(@jobID,N''))) = N'')
						BEGIN
							SET @continue = 0;
							SET @msg      = N'The Job does not exists';
						END 
			END

	--JOB ACTIONS
		IF(@continue = 1 AND @action = N'status')
			BEGIN
			--RETURN THE STATUS OF THE JOB
				SELECT
					CASE
						WHEN (a.run_Requested_date IS NOT NULL AND a.stop_execution_date IS NULL) THEN 1
						ELSE 0
					END AS isRunning
					,CASE
						WHEN (a.run_Requested_date IS NOT NULL AND a.stop_execution_date IS NULL) THEN 
							CONVERT(VARCHAR(50),a.run_Requested_date,106) + ' ' + CONVERT(VARCHAR(2),DATEPART(hour,a.run_Requested_date)) + ':' + CONVERT(VARCHAR(2),DATEPART(minute,a.run_Requested_date)) + ' ' + CONVERT(VARCHAR(2),DATEPART(second,a.run_Requested_date)) + 's' 
						ELSE
							CONVERT(VARCHAR(50),a.stop_execution_date,106) + ' ' + CONVERT(VARCHAR(2),DATEPART(hour,a.stop_execution_date)) + ':' + CONVERT(VARCHAR(2),DATEPART(minute,a.stop_execution_date)) + ' ' + CONVERT(VARCHAR(2),DATEPART(second,a.stop_execution_date)) + 's'
					END AS lastRunDate		
					,CASE
						WHEN (a.run_Requested_date IS NOT NULL AND a.stop_execution_date IS NULL) THEN DATEDIFF(second,a.run_Requested_date,GETDATE())
						ELSE DATEDIFF(second,a.run_Requested_date,a.stop_execution_date)
					END AS executionTime
					,CASE
						WHEN (a.run_Requested_date IS NOT NULL AND a.stop_execution_date IS NULL) THEN 'Running'
						WHEN (b.last_run_outcome = 0) THEN 'Failed'
						WHEN (b.last_run_outcome = 1) THEN 'Succeeded'
						WHEN (b.last_run_outcome = 2) THEN 'Retry'
						WHEN (b.last_run_outcome = 3) THEN 'Canceled'
						ELSE                               'Unknown'
					END AS LastRunStatus
				FROM 
					msdb.dbo.sysjobactivity a INNER JOIN msdb.dbo.sysjobsteps b ON
						b.job_id = a.job_id
				WHERE 
					a.job_id = @jobID
					AND a.run_Requested_date = (
						SELECT MAX(run_Requested_date) 
						FROM msdb.dbo.sysjobactivity 
						WHERE job_id = @jobID
					);
			END
		ELSE IF(@continue = 1 AND @action = N'run')
			BEGIN
			--EXECUTE THE JOB
				--CHECKING IF THE JOB IS NOT RUNNING
					IF(
						NOT EXISTS(
							SELECT	1	
							FROM	msdb.dbo.sysjobactivity a
							WHERE 
								    a.run_Requested_date  IS NOT NULL  
								AND a.stop_execution_date IS NULL  
						)
					)
						BEGIN
							BEGIN TRANSACTION
							BEGIN TRY
								EXEC msdb.dbo.sp_start_job @job_id = @jobID;
								
								SET @msg = N'Job running';
								
								COMMIT TRANSACTION;
							END TRY
							BEGIN CATCH
								SET @continue = 0;
								SET @msg = N'Error.01 - Line: ' + CONVERT(NVARCHAR(10),ERROR_LINE())+ N' | ' + ERROR_MESSAGE();
								ROLLBACK TRANSACTION;
							END CATCH
						END
					ELSE
						BEGIN
							SET @continue = 0;
							SET @msg      = N'Job already running';
						END
			END
		ELSE IF(@continue = 1 AND @action = N'stop')
			BEGIN
			--STOP THE JOB
				--CHECK IF THE JOB IS RUNNING
					IF(
						EXISTS(
							SELECT	1	
							FROM	msdb.dbo.sysjobactivity a
							WHERE 
								    a.run_Requested_date  IS NOT NULL  
								AND a.stop_execution_date IS NULL  
						)
					)
						BEGIN
							BEGIN TRANSACTION
							BEGIN TRY
								EXEC msdb.dbo.sp_stop_job @job_id = @jobID;
								
								SET @msg = N'Job stopped';
								
								COMMIT TRANSACTION;
							END TRY
							BEGIN CATCH
								SET @continue = 0;
								SET @msg = N'Error.02 - Line: ' + CONVERT(NVARCHAR(10),ERROR_LINE())+ N' | ' + ERROR_MESSAGE();;
								ROLLBACK TRANSACTION;
							END CATCH
						END
					ELSE
						BEGIN
							SET @continue = 0;
							SET @msg      = N'Job is not running';
						END
			END
	
	--RESULT IF @action IS NOT status
		IF (NOT @action = N'status')
			BEGIN 
				SELECT 
					 @continue AS result
					,@msg      AS msg;
			END
END
GO
