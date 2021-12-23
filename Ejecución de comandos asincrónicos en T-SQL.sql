IF SCHEMA_ID('async') IS NULL
BEGIN
	DECLARE @SQL varchar(max) = 'CREATE SCHEMA async'
	EXECUTE (@SQL)
END
GO

IF OBJECT_ID('[async].[SpoolConfiguration]') IS NULL
BEGIN
	CREATE TABLE [async].[SpoolConfiguration](
		[ExecutionSpoolName] [varchar](50) NOT NULL,
		[InstanceNameTemplate] [varchar](255) NOT NULL,
		[NumberExecutionAgents] [int] NOT NULL DEFAULT ((1)),
		[PendingCommands] [int] NOT NULL DEFAULT ((0)),
		[DaysToKeepProcessedCommands] [int] NOT NULL DEFAULT ((1)),
		[AgentsStarted] [bit] NOT NULL DEFAULT ((0)),
		[Enabled] [bit] NOT NULL DEFAULT ((1)),
	 CONSTRAINT [PK_SpoolConfiguration] PRIMARY KEY CLUSTERED 
	(
		[ExecutionSpoolName] ASC
	)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
	) ON [PRIMARY]
END	
GO

IF OBJECT_ID('[async].[CommandsHistory]') IS NULL
BEGIN
	CREATE TABLE [async].[CommandsHistory](
		[IdAsyncCommand] [bigint] IDENTITY(1,1) NOT NULL,
		[Command] [varchar](max) NOT NULL,
		[RegisteredOn] [datetime2] NOT NULL,
		[Login] [varchar](255) NOT NULL,
		[SQLUser] [varchar](255) NOT NULL,
		[ClientNetAddress] [varchar](50) NOT NULL,
		[ExecutionSpoolName] [varchar](50) NOT NULL,
		[StartedOn] [datetime2] NULL,
		[FinishedOn] [datetime2] NULL,
		[FailedOn] [datetime2] NULL,
		[ExecutionStatus] [varchar](50) NOT NULL,
		[ErrorMessage] [varchar](500) NULL,
		[OutputInformation] [varchar](max) NULL,
		[Metadata] [varchar](500) NULL,
	 CONSTRAINT [PK_CommandsHistory] PRIMARY KEY CLUSTERED 
	(
		[IdAsyncCommand] ASC
	)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
	) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];

	CREATE NONCLUSTERED INDEX [IX_CommandsHistory] ON [async].[CommandsHistory]
	(
		[ExecutionSpoolName] ASC
	)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY];

	ALTER TABLE [async].[CommandsHistory]  WITH CHECK ADD  CONSTRAINT [FK_CommandsHistory_SpoolConfiguration] FOREIGN KEY([ExecutionSpoolName])
		REFERENCES [async].[SpoolConfiguration] ([ExecutionSpoolName]);

	ALTER TABLE [async].[CommandsHistory]  WITH CHECK ADD  CONSTRAINT [CK_CommandsHistory] 
		CHECK  (([ExecutionStatus]='Failed' OR [ExecutionStatus]='Finished' OR [ExecutionStatus]='Started' OR [ExecutionStatus]='Not Started'))
END
GO
IF (SELECT COUNT(*) FROM [async].[SpoolConfiguration]) = 0
	INSERT INTO [async].[SpoolConfiguration]
		(ExecutionSpoolName, InstanceNameTemplate, NumberExecutionAgents, PendingCommands, DaysToKeepProcessedCommands, AgentsStarted, Enabled)
		VALUES
		('DEFAULTPARALLEL', 'AsyncCmds-<<Database>>-DEFAULTPARALLEL-I<<InstanceNumber>>', 4, 0, 1, 1, 1)
		,('DEFAULTQUEUE', 'AsyncCmds-<<Database>>-DEFAULTQUEUE', 1, 0, 1, 1, 1)
GO

/*
Returns the status of the execution of a command identified by @pIdAsyncCommand
*/
CREATE OR ALTER FUNCTION async.GetCmdExecStatus(@pIdAsyncCommand bigint)
RETURNS TABLE
AS RETURN
(
	SELECT 
		* 
		FROM async.CommandsHistory 
		WHERE IdAsyncCommand = @pIdAsyncCommand
)
GO
/*
Returns the number of agents currently running for a specific ExecutionSpoolName
*/
CREATE OR ALTER FUNCTION async.NumOfAgentsRunning (@pExecutionSpoolName varchar(50))
RETURNS int
WITH EXECUTE AS CALLER
AS
BEGIN
	DECLARE @Template varchar(255) = REPLACE(REPLACE((SELECT InstanceNameTemplate FROM async.SpoolConfiguration WHERE ExecutionSpoolName = @pExecutionSpoolName)
										, '<<InstanceNumber>>', '')
										, '<<Database>>', DB_NAME())
	DECLARE @r int = (-- COUNTS the Agents currently running
						SELECT COUNT(*)
							FROM msdb.dbo.sysjobs_view job
								INNER JOIN msdb.dbo.sysjobactivity act
									ON job.job_id = act.job_id
								INNER JOIN msdb.dbo.syssessions sess
									ON sess.session_id = act.session_id
								INNER JOIN (SELECT max_agent_start_date= MAX( agent_start_date ) 
												FROM msdb.dbo.syssessions) sess_max
									ON sess.agent_start_date = sess_max.max_agent_start_date
							WHERE run_requested_date IS NOT NULL AND stop_execution_date IS NULL
							AND job.name like @Template + '%') 
					
	RETURN (@r)
END
GO

/*
Cycles continuosly looking for commands to execute for a specific SpoolName
It ends when AgentsStarted is set to 0 for the ExecutionSpoolName
*/
CREATE OR ALTER PROCEDURE async.PollPendingCommands
@pExecutionSpoolName varchar(50) = 'DEFAULTQUEUE'
AS
BEGIN
	SET NOCOUNT ON 

	WHILE (SELECT AgentsStarted FROM async.SpoolConfiguration WHERE ExecutionSpoolName = @pExecutionSpoolName) = 1
	BEGIN
		WHILE (SELECT PendingCommands FROM async.SpoolConfiguration WHERE ExecutionSpoolName = @pExecutionSpoolName) > 0
			EXECUTE async.ProcessSpooledCmd @pExecutionSpoolName

		WAITFOR DELAY '00:00:10'
	END
END
GO

/*
Processes a command stored on CommandsHistory. The ExecutionSpoolName groups a set of pending commands on this table.
This procedures processes the first command 'Not Started' from the ExecutionSpoolName. When it finish, the execution status
is set to 'Finished', and if it fails, is set to 'Failed' and the error reported is registered on the ErrorMessage column
*/
CREATE OR ALTER PROCEDURE async.ProcessSpooledCmd
@pExecutionSpoolName varchar(50) = 'DEFAULTQUEUE'
AS
BEGIN
	SET NOCOUNT ON 

	DECLARE @CRLF varchar(2) = CHAR(13) + CHAR(10)
	DECLARE @CmdTable table (
		IdAsyncCommand bigint
		,Command varchar(max)
		,SQLUser varchar(255)
		,StartedOn datetime2
		,ExecutionStatus varchar(50)
		,OutputInformation varchar(max)
	)

	-- Updates the status, registers the start time, and obtains the data needed for the execution
	-- SO, it fetchs the first available command for the SpoolName
	UPDATE C SET 
		StartedOn = GetUTCDate()
		,ExecutionStatus = 'Started'
		,OutputInformation = 'ExecutionStatus = ''Started'' (' + CONVERT(varchar, GetUTCDate(), 121) + ')'
		OUTPUT 
			inserted.IdAsyncCommand
			,inserted.Command
			,inserted.SQLUser
			,inserted.StartedOn
			,inserted.ExecutionStatus
			,inserted.OutputInformation
			INTO @CmdTable	
		FROM async.CommandsHistory C WITH (ROWLOCK, XLOCK)
		WHERE idAsyncCommand = (SELECT
									IdAsynCommand = MIN(CH.IdAsyncCommand)
									FROM async.SpoolConfiguration SC
										INNER JOIN async.CommandsHistory CH WITH (ROWLOCK, XLOCK)
											ON SC.ExecutionSpoolName = CH.ExecutionSpoolName
									WHERE SC.Enabled = 1
									AND CH.ExecutionStatus = 'Not Started'
									AND SC.ExecutionSpoolName = @pExecutionSpoolName)

	IF @@ROWCOUNT = 1
	BEGIN
		-- There is a command to process
		DECLARE @Cmd varchar(max)
				,@StartedOn datetime2 = (SELECT StartedOn FROM @CmdTable)
				,@SQLUser varchar(255) = (SELECT SQLUser FROM @CmdTable)
			
		SELECT @Cmd = '
		DECLARE @CRLF varchar(2) = CHAR(13) + CHAR(10)
		BEGIN TRY
			BEGIN TRAN
			/* Sets the execution context to the SQL user that submitted the command */
			EXECUTE AS USER = ''' + @SQLUser + '''

			' + (SELECT Command FROM @CmdTable) + '

			REVERT	/* Reverts the execution context back to the original user */

			/* Updates the CommandsHistory table with the Finished status */
			UPDATE C SET 
				ExecutionStatus = ''Finished''
				,FinishedON = GetUTCDate()
				,OutputInformation += @CrLf + ''ExecutionStatus = ''''Finished'''' ('' + CONVERT(varchar, GetUTCDate(), 121) + '')'' + 
										@CRLF + ''Duration (ms): '' + CONVERT(varchar, DATEDIFF_BIG(ms, ''' + CONVERT(varchar, @StartedOn, 121) + ''', GETUTCDATE()))
				FROM async.CommandsHistory C WITH (ROWLOCK, XLOCK)
				WHERE idAsyncCommand = ' + CONVERT(varchar, (SELECT idAsyncCommand FROM @CmdTable)) + '

			COMMIT TRAN
		END TRY
		BEGIN CATCH
			ROLLBACK TRAN
			REVERT	/* Reverts the execution context back to the original user */

			DECLARE @ErrorMessage NVARCHAR(4000) = convert(nvarchar(10), ERROR_NUMBER()) + N'':'' + 
							ERROR_MESSAGE() + N'' Línea: '' + 
							convert(nvarchar(10), ERROR_LINE() - 7 ) + N''. Procedimiento: '' + 
							COALESCE(ERROR_PROCEDURE(), ''BATCH'')
					,@ErrorSeverity INT  = ERROR_SEVERITY()
					,@ErrorState INT = ERROR_STATE()

			/* Updates the CommandsHistory table with the Failed status */
			UPDATE C SET 
				ExecutionStatus = ''Failed''
				,FailedON = GetUTCDate()
				,OutputInformation += @CrLf + ''ExecutionStatus = ''''Failed'''' ('' + CONVERT(varchar, GetUTCDate(), 121) + '')'' + 
										@CRLF + ''Duration until failure (ms): '' + CONVERT(varchar, DATE-DIFF_BIG(ms, ''' + CONVERT(varchar, @StartedOn, 121) + ''', GETUTCDATE()))
				,ErrorMessage = @ErrorMessage + 
								@CRLF + ''Gravedad (Severity): '' + CONVERT(varchar, @ErrorSeverity) +
								@CRLF + ''Estado (State): '' + CONVERT(varchar, @ErrorState)
				FROM async.CommandsHistory C WITH (ROWLOCK, XLOCK)
				WHERE idAsyncCommand = ' + CONVERT(varchar, (SELECT idAsyncCommand FROM @CmdTable)) + '
		END CATCH'

		-- PRINT @Cmd
		EXECUTE (@Cmd)

		UPDATE async.SpoolConfiguration SET PendingCommands -= 1 WHERE ExecutionSpoolName = @pExecutionSpoolName

		IF (SELECT ExecutionStatus FROM async.CommandsHistory WHERE idAsyncCommand = (SELECT idAsyncCommand FROM @CmdTable)) = 'Started'
			UPDATE C SET 
				ExecutionStatus = 'Failed'
				,FailedON = GetUTCDate()
				,OutputInformation += @CrLf + 'ExecutionStatus = ''Failed'' (' + CONVERT(varchar, GetUTCDate(), 121) + ')' + 
										@CRLF + 'Duration until failure (ms): ' + CONVERT(varchar, DATEDIFF_BIG(ms, @StartedOn, GETUTCDATE()))
				,ErrorMessage = ''
				FROM async.CommandsHistory C WITH (ROWLOCK, XLOCK)
				WHERE idAsyncCommand =(SELECT idAsyncCommand FROM @CmdTable)
	END -- There was a command to process

	-- Purges commands older than DaysToKeepProcessedCommands
	DELETE CH
		FROM async.CommandsHistory CH WITH (ROWLOCK, XLOCK)
			INNER JOIN async.SpoolConfiguration SC
				ON CH.ExecutionSpoolName = SC.ExecutionSpoolName
		WHERE COALESCE(CH.FinishedOn, CH.FailedOn) < DATEADD(Day, -SC.DaysToKeepProcessedCommands, GETUTCDATE())
		AND CH.ExecutionSpoolName = @pExecutionSpoolName
END
GO
/*
Gets the number of pending commands for an ExecutionSpoolName
*/
CREATE OR ALTER FUNCTION async.NumOfPendingCommands (@pExecutionSpoolName varchar(50))
RETURNS int
WITH EXECUTE AS CALLER
AS
BEGIN
	DECLARE @r int = (SELECT PendingCommands FROM async.SpoolConfiguration WHERE ExecutionSpoolName = @pExecutionSpoolName )
	RETURN @r
END
GO

/*
Returns the pending commands of a ExecutionSpoolName
*/
CREATE OR ALTER FUNCTION async.GetPendingCommands(@pExecutionSpoolName varchar(50))
RETURNS TABLE
AS RETURN
(
	SELECT 
		* 
		FROM async.CommandsHistory 
		WHERE ExecutionSpoolName = @pExecutionSpoolName
		AND ExecutionStatus IN ('Not Started')
)
GO

/*
Returns the executed commands of a ExecutionSpoolName
*/
CREATE OR ALTER FUNCTION async.GetExecutedCommands(@pExecutionSpoolName varchar(50))
RETURNS TABLE
AS RETURN
(
	SELECT 
		* 
		FROM async.CommandsHistory 
		WHERE ExecutionSpoolName = @pExecutionSpoolName
		AND ExecutionStatus NOT IN ('Not Started')
)
GO

/*
Stops the SQL Server Agent jobs (Agentes) for a specific ExecutionSpoolName
This is done indirectly by setting AgentsStarted = 0. When this happens, the procedure async.PollPendingCommands
used by the agents, should stop running after executing the whole set of commands in the ExecutionS-poolName
*/
CREATE OR ALTER PROCEDURE async.StopAgents
@pExecutionSpoolName varchar(50) = 'DEFAULTQUEUE'
AS
BEGIN
	SET NOCOUNT ON
	-- The condition for the agents to run is AgentsStarted = 1. If set to 0 the agents should stop
	UPDATE async.SpoolConfiguration SET AgentsStarted = 0 WHERE ExecutionSpoolName = @pExecutionSpoolName

	IF async.NumOfPendingCommands(@pExecutionSpoolName) > 0
		PRINT 'Los agentes de ''' + @pExecutionSpoolName + ''' se detendrán tan pronto como sean procesados todos los comandos pendientes. Comandos pendientes: ' + CONVERT(varchar, async.NumOfPendingCommands(@pExecutionSpoolName))
END
GO
/*
This procedure makes sure the Agents exist to process the commands sent to a specific ExecutionS-poolName.
So, it creates the jobs if don't exists, and start the jobs if they are not running
*/
CREATE OR ALTER PROCEDURE async.StartAgents
@pExecutionSpoolName varchar(50) = 'DEFAULTQUEUE'
AS
BEGIN
	BEGIN TRY
		IF NOT EXISTS(SELECT * FROM async.SpoolConfiguration WHERE ExecutionSpoolName = @pExecutionSpoolName AND Enabled = 1)
			RAISERROR('EL ExecutionSpoolName ''%s'' no se encuentra activo, por lo tanto los agentes no pueden ser iniciados.', 16, 1, @pExecutionSpoolName)

		IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = DB_NAME() AND containment_desc = 'NONE')
			RAISERROR('La propiedad Containment type de la base de datos actual debe ser ''NONE''.', 16, 1)

		IF NOT EXISTS(SELECT * FROM sys.sysprocesses WHERE program_name = N'SQLAgent - Generic Refresher')
			RAISERROR('Es necesario que SQL Server Agent esté disponible para esta instancia de SQL Server para poder iniciar los agentes.', 16, 1)
	END TRY
	BEGIN CATCH
		RETURN
	END CATCH

	DECLARE @NumAgents int = (SELECT NumberExecutionAgents FROM async.SpoolConfiguration WHERE ExecutionSpoolName = @pExecutionSpoolName)
			,@Template varchar(255) = (SELECT InstanceNameTemplate FROM async.SpoolConfiguration WHERE ExecutionSpoolName = @pExecutionSpoolName)
			,@JobName nvarchar(255)
			,@jobId binary(16)
			,@CurrentAgentInstance int = 1
			,@StepCommand nvarchar(max) = N'EXECUTE async.PollPendingCommands ''' + @pExecutionSpoolName + ''''
			,@CurrentDatabase sysname = DB_NAME()
			
	WHILE @CurrentAgentInstance <= @NumAgents /* Number of agents registered on the configuration table */
	BEGIN
		SET @JobName = REPLACE(REPLACE(@Template 
								,N'<<InstanceNumber>>', RIGHT(N'000' + CONVERT(nvarchar, @CurrentAgentInstance), 4))
								,N'<<Database>>', DB_NAME())
		SET @jobId = NULL
		SELECT @jobId = job_id FROM msdb.dbo.sysjobs_view WHERE name = @JobName

		IF @jobId IS NOT NULL
		BEGIN
			-- The Job (or Agent) exists
			IF NOT EXISTS(-- Checks if the job is running
							SELECT 0
								FROM msdb.dbo.sysjobs_view job
									INNER JOIN msdb.dbo.sysjobactivity act
										ON job.job_id = act.job_id
									INNER JOIN msdb.dbo.syssessions sess
										ON sess.session_id = act.session_id
									INNER JOIN (SELECT max_agent_start_date= MAX( agent_start_date ) 
													FROM msdb.dbo.syssessions) sess_max
										ON sess.agent_start_date = sess_max.max_agent_start_date
								WHERE run_requested_date IS NOT NULL AND stop_execution_date IS NULL
								AND job.job_id = @jobId)
			BEGIN
				-- The Agent exists but IS NOT RUNNING, so it STARTS the job
				UPDATE async.SpoolConfiguration SET AgentsStarted = 1 WHERE ExecutionSpoolName = @pExecutionSpoolName
				EXEC msdb.dbo.sp_start_job  @job_id = @jobId
			END
		END
		ELSE
		BEGIN
			-- The Agent does not exists, it has to be created and run
			IF NOT EXISTS(SELECT * FROM msdb.dbo.syscategories WHERE name = '[async-Agent]')
				EXEC msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL',  @name=N'[async-Agent]' ;

			EXEC  msdb.dbo.sp_add_job @job_name = @JobName, 
				@enabled=1, @notify_level_eventlog=0, @notify_level_email=2, @notify_level_page=2, @delete_level=0, @category_name=N'[async-Agent]', @owner_login_name=N'NT SERVICE\SQLSERVERAGENT', @job_id = @jobId OUTPUT
			
			EXEC msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = @@SERVERNAME

			EXEC msdb.dbo.sp_add_jobstep @job_id = @jobId, @step_name = @JobName, /* Same name as the job */
				@step_id=1, @cmdexec_success_code=0, @on_success_action=1, @on_fail_action=2, @retry_attempts=0, @retry_interval=0, @os_run_priority=0, @subsystem=N'TSQL', 
				@command = @StepCommand, 
				@database_name = @CurrentDatabase, 
				@flags=0

			EXEC msdb.dbo.sp_update_job @job_id = @jobId, 
				@enabled=1, @start_step_id=1, @notify_level_eventlog=0, @notify_level_email=2, @notify_level_page=2, @delete_level=0, 
				@description=N'Agente de Spool que ejecuta comandos almacenados en async.CommandsHistory', 
				@category_name=N'[async-Agent]', 
				@owner_login_name=N'NT SERVICE\SQLSERVERAGENT', 
				@notify_email_operator_name=N'', 
				@notify_page_operator_name=N''

			-- Once the job is created it STARTS 
			UPDATE async.SpoolConfiguration SET AgentsStarted = 1 WHERE ExecutionSpoolName = @pExecutionSpoolName
			EXEC msdb.dbo.sp_start_job @job_id = @jobId
		END

		SET @CurrentAgentInstance += 1
	END -- WHILE

	-- It is possible that the number of agents (NumberExecutionAgents) for a ExecutionSpoolName is redu-ced using the procedure 
	-- async.SetSpoolConfiguration. In this case, the remaining jobs will be deleted.
	DECLARE @JobNameTplt sysname = REPLACE(REPLACE(@Template 
											,N'<<InstanceNumber>>', '')
											,N'<<Database>>', DB_NAME())
	IF @NumAgents < (SELECT COUNT(*) FROM msdb.dbo.sysjobs_view WHERE name like @JobNameTplt + '%') /* Number of agents created on SQL Server Agent */
	BEGIN
		-- There are remaining jobs
		SET @NumAgents = (SELECT COUNT(*) FROM msdb.dbo.sysjobs_view WHERE name like @JobNameTplt + '%') /* Number of agents created on SQL Server Agent */

		WHILE @CurrentAgentInstance <= @NumAgents
		BEGIN
			SET @JobName = REPLACE(REPLACE(@Template 
								,N'<<InstanceNumber>>', RIGHT(N'000' + CONVERT(nvarchar, @CurrentAgentInstance), 4))
								,N'<<Database>>', DB_NAME())
			EXEC msdb.dbo.sp_delete_job @job_name = @JobName

			SET @CurrentAgentInstance += 1
		END
	END
END
GO
/*
Registers a command for asynchronous execution. By default, @pIndependent = 0 means that the commands follow an order, hence
they are put on a queue for later execution. On the contrary, when @pIndependent = 1, the commands are independent among them,
so they can be run in parallel.
The procedure assumes the current Database Context
*/
CREATE OR ALTER PROCEDURE async.ExecuteCmd 
@pCmd varchar(max)
,@pIsIndependent bit = 0
,@pNonDefaultExecutionSpoolName varchar(50) = NULL
AS
BEGIN
	SET NOCOUNT ON 

	-- Initialize the variables with their default values
	DECLARE
		@ExecutionSpoolName varchar(50) = COALESCE(@pNonDefaultExecutionSpoolName, IIF(@pIsIndependent = 0, 'DEFAULTQUEUE', 'DEFAULTPARALLEL'))
		,@RegisteredOn datetime2 = GetUTCDate()
		,@Login varchar(255) = SUSER_SNAME()
		,@SQLUser varchar(255) = USER_NAME()
		,@ClientNetAddress varchar(50) = (SELECT TOP 1 client_net_address FROM sys.dm_exec_connections WHERE most_recent_session_id = @@SPID)
		,@SQL varchar(max) 
		
	-- Tries to make sure that the Agents for the ExecutionSpoolName are running
	-- so they can process the command sent
	BEGIN TRY
		IF (SELECT Enabled FROM async.SpoolConfiguration WHERE ExecutionSpoolName = @ExecutionSpoolName) = 0
			RAISERROR ('El ExecutionSpoolName ''%s'' no está habilitado. El comando NO ha sido registrado y por lo tanto NO será ejecutado.', 16, 1, @ExecutionSpoolName)

		IF async.NumOfAgentsRunning(@ExecutionSpoolName) <> (SELECT NumberExecutionAgents FROM async.SpoolConfiguration WHERE ExecutionSpoolName = @ExecutionSpoolName)
			-- Tries to start up the full set of agents for the specific ExecutionSpoolName if is not complete
			EXECUTE async.StartAgents @ExecutionSpoolName

		IF async.NumOfAgentsRunning(@ExecutionSpoolName) = 0
			RAISERROR ('El ExecutionSpoolName de tipo ''%s'' no tiene agentes funcionando. El comando NO ha sido registrado y por lo tanto NO será ejecutado.', 16, 1, @ExecutionSpoolName)
		ELSE
		BEGIN
			-- The SET PARSEONLY only check syntax
			SET @SQL = 'SET PARSEONLY ON; SET NOCOUNT ON; ' + @pCmd;
			EXECUTE (@SQL);

			-- Inserts the command in the table for later execution if the command is valid
			IF @@ERROR = 0
			BEGIN
				BEGIN TRAN
				INSERT INTO [async].[CommandsHistory]
						([Command], [RegisteredOn], [Login], [SQLUser], [ClientNetAddress], [ExecutionSpoolName], [ExecutionStatus])
					OUTPUT inserted.IdAsyncCommand
					VALUES
						(@pCmd, @RegisteredOn, @Login, @SQLUser, @ClientNetAddress, @ExecutionSpoolName, 'Not Star-ted')
		
				UPDATE async.SpoolConfiguration SET PendingCommands += 1 WHERE ExecutionSpoolName = @ExecutionSpoolName
				COMMIT TRAN

				PRINT 'El comando SQL ha sido registrado para ser ejecutado posteriormente.'
			END
		END
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT >= 1 ROLLBACK TRANSACTION

		DECLARE @ErrorMessage NVARCHAR(4000) = convert(nvarchar(10), ERROR_NUMBER()) + N':' + 
						ERROR_MESSAGE() + N' Linea: ' + 
						convert(nvarchar(10), ERROR_LINE()) + N'. Procedimiento: ' + 
						ERROR_PROCEDURE()
				,@ErrorSeverity INT  = ERROR_SEVERITY()
				,@ErrorState INT = ERROR_STATE()

		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
	END CATCH
END
GO

/*
Deletes a non default ExecutionSpoolName and their agents
*/
CREATE OR ALTER PROCEDURE async.DropSpool
@pExecutionSpoolName varchar(50)
AS
BEGIN
	SET NOCOUNT ON

	DECLARE @Template varchar(255) = (SELECT InstanceNameTemplate FROM async.SpoolConfiguration WHERE ExecutionSpoolName = @pExecutionSpoolName)
	BEGIN TRY
		IF async.NumOfPendingCommands(@pExecutionSpoolName) > 0
			RAISERROR ('El ExecutionSpoolName ''%s'' tiene comandos pendientes por ejecutar y por lo tanto no puede ser borrado.', 16, 1, @pExecutionSpoolName)

		IF @pExecutionSpoolName IN ('DEFAULTPARALLEL', 'DEFAULTQUEUE')
			RAISERROR ('El ExecutionSpoolName ''%s'' es uno de los predefinidos por defecto y por lo tanto no puede ser borrado.', 16, 1, @pExecutionSpoolName)

		EXECUTE async.StopAgents @pExecutionSpoolName

		DECLARE @JobName sysname
				,@JobNameTplt sysname = REPLACE(REPLACE(@Template 
											,N'<<InstanceNumber>>', '')
											,N'<<Database>>', DB_NAME())
		DECLARE @Instance int = 1
				,@NumAgents int = (SELECT COUNT(*) FROM msdb.dbo.sysjobs_view WHERE name like @JobNameTplt + '%')
		
		WHILE @Instance <= @NumAgents
		BEGIN
			SET @JobName = REPLACE(REPLACE(@Template 
								,N'<<InstanceNumber>>', RIGHT(N'000' + CONVERT(nvarchar, @Instance), 4))
								,N'<<Database>>', DB_NAME())
			EXEC msdb.dbo.sp_delete_job @job_name = @JobName

			SET @Instance += 1
		END

		BEGIN TRAN
		DELETE FROM async.CommandsHistory WHERE ExecutionSpoolName = @pExecutionSpoolName
		DELETE FROM async.SpoolConfiguration WHERE ExecutionSpoolName = @pExecutionSpoolName
		COMMIT TRAN
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT >= 1 ROLLBACK TRANSACTION

		DECLARE @ErrorMessage NVARCHAR(4000) = convert(nvarchar(10), ERROR_NUMBER()) + N':' + 
						ERROR_MESSAGE() + N' Linea: ' + 
						convert(nvarchar(10), ERROR_LINE()) + N'. Procedimiento: ' + 
						ERROR_PROCEDURE()
				,@ErrorSeverity INT  = ERROR_SEVERITY()
				,@ErrorState INT = ERROR_STATE()

		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
	END CATCH
END
GO

/*
Establishes the configuration for a new or existing ExecutionSpoolName. If it does not exists it creates a new one.
*/
CREATE OR ALTER PROCEDURE async.SetSpoolConfiguration
@pExecutionSpoolName varchar(50)
,@pNumberExecutionAgents int = NULL
,@pDaysToKeepProcessedCommands int = 1
,@pEnabled bit = 1
AS
BEGIN
	SET NOCOUNT ON

	IF @pExecutionSpoolName = 'DEFAULTQUEUE' AND @pNumberExecutionAgents <> 1
		RAISERROR('La cola por defecto (DEFAULTQUEUE) solo puede tener un agente debido a que está destinada a ser serial', 16, 1)

	IF COALESCE(async.NumOfPendingCommands(@pExecutionSpoolName), 0) = 0
		MERGE INTO async.SpoolConfiguration T
			USING (SELECT 
						ExecutionSpoolName = @pExecutionSpoolName
						,InstanceNameTemplate = 'AsyncCmds-<<Database>>-' + @pExecutionSpoolName + '-I<<InstanceNumber>>'
						,NumberExecutionAgents = COALESCE(@pNumberExecutionAgents, (SELECT cpu_count FROM sys.dm_os_sys_info))
						,DaysToKeepProcessedCommands = COALESCE(@pDaysToKeepProcessedCommands, 1)
						,Enabled = COALESCE(@pEnabled, 1) ) S
				ON T.ExecutionSpoolName = S.ExecutionSpoolName
			WHEN MATCHED THEN UPDATE SET
				ExecutionSpoolName = S.ExecutionSpoolName
				,InstanceNameTemplate = S.InstanceNameTemplate
				,NumberExecutionAgents = S.NumberExecutionAgents
				,DaysToKeepProcessedCommands = S.DaysToKeepProcessedCommands
				,Enabled = S.Enabled
			WHEN NOT MATCHED THEN INSERT
				(ExecutionSpoolName, InstanceNameTemplate, NumberExecutionAgents, DaysToKeepProcessedCommands, Enabled)
				VALUES 
				(S.ExecutionSpoolName, S.InstanceNameTemplate, S.NumberExecutionAgents, S.DaysToKeepProcessedCommands, S.Enabled);
	ELSE
		RAISERROR('Hay comandos pendientes por ejecutar, por ahora no es posible cambiar la configuración del ExecutionSpoolName ''%s''. Intente más tarde.', 16, 1, @pExecutionSpoolName)
END
GO

