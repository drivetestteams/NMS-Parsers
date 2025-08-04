# The Parser application controller
# 3skelion_parser.py
# RESUME

This file contains all necessary components (functions, scheduler) for the control and synchronization of the file parser scripts.
It generates a GUI for on-demand execution by the system's administrator, which also keeps a track of the last execution outcome and timestamp.
The execution of the individual parser scripts is performed in threads to achieve seamless and unobstructed running.
In case of a script failure a notification sending logic has been developed (by email) to a specific list of individuals.

Every process included in this file is logged and exported into equivalent logfiles.

================================ FUNCTIONS ================================
** ALL FUNCTIONS ARE INCLUDED IN THE CLASS SimpleSchedulerApp **
# def __init__(self, root)

Arguments:
- self: instance of the class SimpleSchedulerApp is run.
- root: a class level variable

This function initializes all necessary components for the parsers' processes.
It opens and runs the CMS software components: FTP_synch and Telemetry.
Opens the GUI used by the system administrator, with default values being depicted regarding the Last Run, Next Run, and Last Run Exit Code.
Finally, it schedules the initial run of each parser script and the whole program restart. Time schedules can be adjusted on-demand.

# def kill_self_after_timeout(thread, timeout=90000)

Arguments:
- thread: the thread under monitor executing a specific parser script.
- timeout=90000: the timeout limit, for which the scheduler has to wait for the thread to be executed.

This function monitors the thread provided in the arguments and waits for it to be executed for a duration of 90000 ms default value.
After this timeout is surpassed the scheduler kills the thread to avoid conflict between executions.

# def restart_program(self)

Arguments:
- self: instance of the class SimpleSchedulerApp is run.

This function restarts the program by terminating all subprocesses and launching a new instance of the script.
It also ensures that the GUI and Telemetry App is closed properly before restarting.

# def set_icon(self, icon_path)

Arguments:

- self: instance of the class SimpleSchedulerApp is run.
- icon_path: the path of the icon used to decorate the GUI of the parsers.

This function simply sets the window icon to decorate the GUI of the parsers.

# def start_telemetry(self)

Arguments:

- self: instance of the class SimpleSchedulerApp is run.

This function starts the Telemetry.exe process and returns the process object.
It handles any exceptions that may occur during the process start.

# def start_auto_sync(self)

Arguments:

- self: instance of the class SimpleSchedulerApp is run.

This function starts the FTP_sync.exe process and returns the process object.
It handles any exceptions that may occur during the process start.

# def on_closing(self)

Arguments:

- self: instance of the class SimpleSchedulerApp is run.

This function handles the closing of the application.
It ensures that both Telemetry.exe and Auto_Sync.exe processes are terminated properly.

# def restart_telemetry(self)

Arguments:

- self: instance of the class SimpleSchedulerApp is run.

This function restarts the Telemetry.exe process.
It first terminates the existing process and then starts a new instance.

# def create_widgets(self)

Arguments:

- self: instance of the class SimpleSchedulerApp is run.

This function creates the GUI widgets for the application.
It sets up the layout, styles, and buttons for each parser script.

# def run_script_in_thread(self, script_name)

Arguments:

- self: instance of the class SimpleSchedulerApp is run.
- script_name: the name of the script under execution.

This function runs a script in a separate thread, using the run_script() function.
It handles the script name and starts the thread for execution.
It also sets a timeout for the thread to ensure it does not run indefinitely, using the function kill_self_after_timeout().

# def run_script(self, script_name)

Arguments:

- self: instance of the class SimpleSchedulerApp is run.
- script_name: the name of the script under execution.

This function runs the specified script.
It uses subprocess to execute the script and captures its output.
It also handles the exit code and logs the output to files in a separate logfile, which is stored in the running machine local disk.

# def schedule_next_run_in_thread(self, script_name, label, next_run_time)

Arguments:

- self: instance of the class SimpleSchedulerApp is run.
- script_name: the name of the script under execution.
- label: label of the individual script on the parsers' GUI.
- next_run_time: next run time of the individual script

This function schedules the next run of a script in a separate thread.

# def schedule_next_run(self, script_name, label, next_run_time)

Arguments:

- self: instance of the class SimpleSchedulerApp is run.
- script_name: the name of the script under execution.
- label: label of the individual script on the parsers' GUI.
- next_run_time: next run time of the individual script.

This function schedules the next run of a script.
It updates the label with the next run time and sets a delay for the next execution.

# def email_report(self, title, content)

Arguments:

- self: instance of the class SimpleSchedulerApp is run.
- title: the title of the mail to be sent.
- content: content of the mail to be sent.

This function sends an email report with the specified title and content.
It uses the smtplib library to connect to an SMTP server and send the email.

================================ MAIN ================================

Initializes the scheduler of the program.
