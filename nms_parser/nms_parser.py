# 3skelion_parser.py
# This script is a simple scheduler application for running various parsers
# It uses tkinter for the GUI and subprocess for running external scripts

import tkinter as tk
from tkinter import ttk
import time
import threading
import subprocess
from datetime import datetime, timedelta
import sys
import os
# Email-related imports were removed because alert emails are disabled for client deployment.

# Paths to the parser scripts and executables
# These paths should be set according to your directory structure
# Note: Ensure that the paths are correct and accessible from the machine running this script
# You can change these paths to absolute paths if needed, but relative paths are used here for portability

# Base directory of this controller script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Absolute script paths so execution does not depend on the current working directory
HISTORIC_SCRIPT = os.path.join(BASE_DIR, "new_historic_parser", "parser_new_hisoric.py")
LIVE_SCRIPT = os.path.join(BASE_DIR, "new_live_parser", "live_parser.py")

# Telemetry executable path
TELEMETRY_EXE = r"C:\Program Files (x86)\FasmetricsSoftware\APP TELEMETRY CLIENT SERVICE\TelemetryService.exe"

# Asset path
ICON_PATH = os.path.join(BASE_DIR, "assets", "FASMETRICS_LOGONLY.ico")

# Log directories
LOG_BASE_DIR = os.path.join(BASE_DIR, "log_files")
LOG_OK_DIR = os.path.join(LOG_BASE_DIR, "EXIT_CODE_0")
LOG_ERR_DIR = os.path.join(LOG_BASE_DIR, "ERROR")

# Ensure log folders exist before any logging happens
os.makedirs(LOG_OK_DIR, exist_ok=True)
os.makedirs(LOG_ERR_DIR, exist_ok=True)

FourSkelion_Historic_path = "./new_historic_parser/parser_new_hisoric"  # Path to the new historic parser script

FourSkelion_Live_path = "./new_live_parser/live_parser"  # Path to the new live parser script

Telemetry_path = "C:\\Program Files (x86)\\FasmetricsSoftware\\APP TELEMETRY CLIENT SERVICE\\TelemetryService.exe"        # Path to the Telemetry Client executable

Auto_sync_path = "C:\\Program Files (x86)\\FasmetricsSoftware\\APP FTP AUTO SYNCH\\FTP_SYNCHRONIZER.exe"   # Path to the Auto Sync executable

# This is the main application class that initializes the GUI and manages the scheduling of scripts
class SimpleSchedulerApp: # Override the class "SimpleSchedulerApp" with these functions. 
    
    def __init__(self, root):
        self.root = root
        self.root.title("NMS Files Parser")

        # Set the window icon
        #self.set_icon("../assets/FASMETRICS_LOGONLY.ico")
        # Load icon only if it exists, to avoid startup failure in case assets are missing on the client VM.
        if os.path.exists(ICON_PATH):
            self.set_icon(ICON_PATH)
        
        # Initialize Telemetry.exe
        self.telemetry_process = self.start_telemetry()
        
        # Initialize Auto_Sync.exe
        self.auto_sync_process = self.start_auto_sync()
        
        # Bind the close event
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Create UI
        self.create_widgets()
        
       # Schedule the worker threads to run after 5 minutes (300,000 milliseconds)
        self.root.after(300000 + 200, self.run_script_in_thread, FourSkelion_Historic_path)
        self.root.after(300000 + 400, self.run_script_in_thread, FourSkelion_Live_path)
        self.root.after(300000 + 9800, self.run_script_in_thread, "Restart_telemetry")
        
        # Schedule program restart every 4 hours (4 * 60 * 60 * 1000 milliseconds)
        self.root.after(4 * 60 * 60 * 1000, self.restart_program)

    # This function monitors the thread and kills the program if it exceeds the timeout   
    # # Watchdog for worker threads:
    # if a parser thread hangs for too long, terminate the controller process
    # so the external BAT / Task Scheduler mechanism can restart it cleanly. 
    def kill_self_after_timeout(self, thread, timeout=900):
        def monitor():
            time.sleep(timeout)
            if thread.is_alive():
                print(f"Timeout reached after {timeout} seconds. Controller will exit for external restart.")
                os._exit(1)

        threading.Thread(target=monitor, daemon=True).start()
            
    # This function restarts the program by terminating all subprocesses and launching a new instance of the script
    # It also ensures that the GUI is closed properly before restarting                
    def restart_program(self):
        """Terminate all processes and restart the program"""
        print("Restarting the program...")
        
        # Terminate subprocesses before restarting
        if self.telemetry_process:
            try:
                self.telemetry_process.terminate()
                self.telemetry_process.wait()
                print(f"Closed Telemetry.exe with PID {self.telemetry_process.pid}")
            except Exception as e:
                print(f"Error closing Telemetry.exe: {e}")
        
        print("Starting Restart process...")
        python = sys.executable
        script = os.path.abspath(sys.argv[0])
        try:
            subprocess.Popen([python, script])  # Start a new instance of the script
        except Exception as e:
            print(f"Failed to restart the script: {e}")
        self.root.destroy()
        time.sleep(1)  # Give the new process a moment to start
        sys.exit()


    # This function sets the window icon for the application
    # It checks if the icon file exists and sets it, otherwise prints an error message
    def set_icon(self, icon_path):
        if os.path.exists(icon_path):
            try:
                self.root.iconbitmap(icon_path)
            except Exception as e:
                print(f"Error setting icon: {e}")
        else:
            print(f"Icon file not found: {icon_path}")


    # This function starts the Telemetry.exe process and returns the process object
    # It handles any exceptions that may occur during the process start
    def start_telemetry(self):
        try:
            telemetry_process = subprocess.Popen([Telemetry_path])
            print(f"Started Telemetry.exe with PID {telemetry_process.pid}")
            return telemetry_process
        except Exception as e:
            print(f"Error starting Telemetry.exe: {e}")
            return None
        
    # This function starts the Auto_sync.exe process and returns the process object
    # It handles any exceptions that may occur during the process start
    def start_auto_sync(self):
        try:
            auto_sync_process = subprocess.Popen([Auto_sync_path])
            print(f"Started Auto_sync.exe with PID {auto_sync_process.pid}")
            return auto_sync_process
        except Exception as e:
            print(f"Error starting Auto_sync.exe: {e}")
            return None

    def close_all_opened_processes(self):
        if self.telemetry_process:
            try:
                self.telemetry_process.terminate()
                self.telemetry_process.wait(timeout=10)
                print(f"Closed Telemetry.exe with PID {self.telemetry_process.pid}")
            except Exception as e:
                print(f"Error closing Telemetry.exe: {e}")
            finally:
                self.telemetry_process = None

        if self.auto_sync_process:
            try:
                self.auto_sync_process.terminate()
                self.auto_sync_process.wait(timeout=10)
                print(f"Closed Auto_Sync.exe with PID {self.auto_sync_process.pid}")
            except Exception as e:
                print(f"Error closing Auto_Sync.exe: {e}")
            finally:
                self.auto_sync_process = None

    # This function handles the closing of the application
    # It ensures that both Telemetry.exe and Auto_Sync.exe processes are terminated properly
    def on_closing(self):
        self.close_all_opened_processes()
        self.root.destroy()
        os._exit(0)

    # This function restarts the Telemetry.exe process
    # It first terminates the existing process and then starts a new instance
    def restart_telemetry(self):
        #close telemetry 
        if self.telemetry_process:
            try:
                self.telemetry_process.terminate()
                self.telemetry_process.wait()
                print(f"Closed Telemetry.exe with PID {self.telemetry_process.pid}")
            except Exception as e:
                print(f"Error closing Telemetry.exe: {e}")
                
        # start telemetry again
        self.telemetry_process = self.start_telemetry()
    
        return 0
    
    # This function creates the GUI widgets for the application
    # It sets up the layout, styles, and buttons for each parser script
    def create_widgets(self):
        # Style for labels
        label_style = ttk.Style()
        label_style.configure("TLabel", font=("Lato", 12, "bold"), padding=(0, 20))

        # Style for buttons
        button_style = ttk.Style()
        button_style.configure("TButton", font=("Lato", 12), padding=(0, 10))

        # Style for smaller labels
        small_label_style = ttk.Style()
        small_label_style.configure("Small.TLabel", font=("Lato", 10), padding=(0, 10))


        # Script 3 Section
        frame3 = ttk.Frame(self.root, padding="20")
        frame3.grid(row=0, column=2, padx=20, pady=20)
        label3 = ttk.Label(frame3, text="Gen4 Historic Data Parser", style="TLabel")
        label3.pack()
        button3 = ttk.Button(frame3, text="Run Now", command=lambda: self.run_script_in_thread(FourSkelion_Historic_path), style="Large.TButton")
        button3.pack()
        self.run_status_label31 = ttk.Label(frame3, text="Last Run: Never", style="Small.TLabel")
        self.run_status_label31.pack()
        self.exit_code_label31 = ttk.Label(frame3, text="Last Run Exit Code: N/A", style="Small.TLabel")
        self.exit_code_label31.pack()
        self.run_status_label32 = ttk.Label(frame3, text="Next Run: Not Scheduled", style="Small.TLabel")
        self.run_status_label32.pack()

        # Script 4 Section
        frame4 = ttk.Frame(self.root, padding="20")
        frame4.grid(row=0, column=3, padx=20, pady=20)
        label4 = ttk.Label(frame4, text="Gen4 Live Data Parser", style="TLabel")
        label4.pack()
        button4 = ttk.Button(frame4, text="Run Now", command=lambda: self.run_script_in_thread(FourSkelion_Live_path), style="Large.TButton")
        button4.pack()
        self.run_status_label41 = ttk.Label(frame4, text="Last Run: Never", style="Small.TLabel")
        self.run_status_label41.pack()
        self.exit_code_label41 = ttk.Label(frame4, text="Last Run Exit Code: N/A", style="Small.TLabel")
        self.exit_code_label41.pack()
        self.run_status_label42 = ttk.Label(frame4, text="Next Run: Not Scheduled", style="Small.TLabel")
        self.run_status_label42.pack()
        
        # Restart Telemetry Script Section
        button5 = ttk.Button(frame4, text="Restart Telemetry", command=lambda: self.run_script_in_thread("Restart_telemetry"), style="Large.TButton")
        button5.pack()
        self.run_status_label53 = ttk.Label(frame4, text="Next Telemetry Restart: Not Scheduled", style="Small.TLabel")
        self.run_status_label53.pack()

        # Close EVERYTHING
        close_all_button = ttk.Button(
            frame3,
            text="Close Everything",
            command=self.on_closing,
            style="Large.TButton"
        )
        close_all_button.pack(pady=10)


    # This function runs a script in a separate thread
    # It handles the script name and starts the thread for execution
    # It also sets a timeout for the thread to ensure it does not run indefinitely
    # Run each managed process in a background thread so the GUI remains responsive.
    # A watchdog thread monitors execution time in case a parser hangs.
    def run_script_in_thread(self, script_name):
        thread = threading.Thread(
            target=self.run_script,
            args=(script_name,),
            daemon=True
        )
        thread.start()
        self.kill_self_after_timeout(thread, timeout=900)
    
    def update_label_safe(self, label, text):
        self.root.after(0, lambda: label.config(text=text))

    # This function runs the specified script
    # It uses subprocess to execute the script and captures its output
    # It also handles the exit code and logs the output to files
    def run_script(self, script_name):
        try:
           
            # if script name is a method run it  
            if script_name == "Restart_telemetry":
                restart_code = self.restart_telemetry()
                class DummyResult:
                    def __init__(self, returncode, stdout="", stderr=""):
                        self.returncode = returncode
                        self.stdout = stdout
                        self.stderr = stderr

                result = DummyResult(restart_code, stdout="Telemetry restarted successfully.", stderr="")
            # Run the external script
            else:
                result = subprocess.run(['python', f'{script_name}.py'], capture_output=True, text=True)
                #That is safer on the client VM, especially if: 1.multiple Python versions exist // 2.PATH is not configured // 3.Task Scheduler launches under a different environment
                #result = subprocess.run([sys.executable, f'{script_name}.py'], capture_output=True, text=True)
                
                # Save one timestamped log file per execution, including the executed path,
                # exit code, and captured stdout/stderr, so troubleshooting is easier on the client VM.
                # Based on the exit code, print the output/error and save it in the correct log folder.
                current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                print(f"The return code is: {result.returncode}")
                if result.returncode == 0:
                    path = LOG_OK_DIR
                    print(result.stdout)

                    safe_script_name = os.path.basename(script_name).replace(' ', '_').replace('.', '_')
                    file_name = os.path.join(path, f"{safe_script_name}_{current_datetime}.txt")

                    with open(file_name, 'w', encoding='utf-8') as file:
                        #file.write(f"EXECUTED PATH: {script_path}\n")
                        file.write(f"EXIT CODE: {result.returncode}\n")
                        file.write("----------STDOUT----------\n")
                        file.write(result.stdout if result.stdout else "(no stdout)\n")

                else:
                    path = LOG_ERR_DIR
                    print(result.stdout)
                    print("_______________________________________")
                    print(result.stderr)

                    safe_script_name = os.path.basename(script_name).replace(' ', '_').replace('.', '_')
                    file_name = os.path.join(path, f"{safe_script_name}_{current_datetime}.txt")
                    print(f"The return code is: {result.returncode}")
                    with open(file_name, 'w', encoding='utf-8') as file:
                        #file.write(f"EXECUTED PATH: {script_path}\n")
                        file.write(f"EXIT CODE: {result.returncode}\n")
                        file.write("----------STDERR----------\n")
                        file.write(result.stderr if result.stderr else "(no stderr)\n")
                        file.write("\n----------STDOUT----------\n")
                        file.write(result.stdout if result.stdout else "(no stdout)\n")

            # Update last run time
            current_time = datetime.now()
                        
            # Update UI labels and schedule next run
                                
            if script_name == FourSkelion_Historic_path:
                #self.run_status_label31.config(text=f"Last Run: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                #self.exit_code_label31.config(text=f"Exit Code: {result.returncode}")

                self.update_label_safe(self.run_status_label31, f"Last Run: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                self.update_label_safe(self.exit_code_label31, f"Exit Code: {result.returncode}")

                next_run_time = current_time + timedelta(minutes=180) # Schedule next run after 3 hours
                self.schedule_next_run_in_thread(script_name, self.run_status_label32, next_run_time) # Schedule next run in a separate thread
                # if this an error happens for the firs time
                # if result.returncode != 0:
                #     self.script_mail_counter3 += 1
                #     if self.script_mail_counter3 == 1:
                #         # send mail that there was an error with the parsers 
                #         title = f"Error with: {script_name}"
                #         body = f"There was an error with {script_name}\n"
                #         self.email_report(title , body)  
                
            elif script_name == FourSkelion_Live_path:
                #self.run_status_label41.config(text=f"Last Run: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                #self.exit_code_label41.config(text=f"Exit Code: {result.returncode}")

                self.update_label_safe(self.run_status_label41, f"Last Run: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                self.update_label_safe(self.exit_code_label41, f"Exit Code: {result.returncode}")

                next_run_time = current_time + timedelta(minutes=2) # Schedule next run after 2 minutes
                self.schedule_next_run_in_thread(script_name, self.run_status_label42, next_run_time) # Schedule next run in a separate thread
                # if this an error happens for the firs time
                # if result.returncode != 0:
                #     self.script_mail_counter4 += 1
                #     if self.script_mail_counter4 == 1:
                #         # send mail that there was an error with the parsers 
                #         title = f"Error with: {script_name}"
                #         body = f"There was an error with {script_name}\n"
                #         self.email_report(title , body)  
                
                
            elif script_name == "Restart_telemetry":
                next_run_time = current_time + timedelta(minutes=60) # Schedule next run after 1 hour
                self.schedule_next_run_in_thread(script_name, self.run_status_label53, next_run_time) # Schedule next run in a separate thread

        except Exception as e:
            print(f"An error occurred: {e}")

    # This function schedules the next run of a script in a separate thread       
    def schedule_next_run_in_thread(self, script_name, label, next_run_time):
        try:
            # Create a thread for scheduling next run
            thread = threading.Thread(target=self.schedule_next_run, args=(script_name, label, next_run_time))
            thread.start()
        except Exception as e:
            print(f"Error starting scheduling thread for {script_name}: {e}")

    # Update the GUI with the next scheduled execution time and register the next run.
    # Label updates are routed through the Tkinter main thread for thread safety.
    # This function schedules the next run of a script
    # It updates the label with the next run time and sets a delay for the next execution
    def schedule_next_run(self, script_name, label, next_run_time):
        delay = max(0, int((next_run_time - datetime.now()).total_seconds() * 1000))

        if label == self.run_status_label53:
            self.update_label_safe(label,f"Next Telemetry Restart: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            self.update_label_safe(label,f"Next Run: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")

        self.root.after(delay, lambda: self.run_script_in_thread(script_name))


if __name__ == "__main__":
    root = tk.Tk()
    app = SimpleSchedulerApp(root)
    root.mainloop()