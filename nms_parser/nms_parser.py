# 3skelion_parser.py
# This script is a simple scheduler application for running various parsers
# It uses tkinter for the GUI and subprocess for running external scripts

import tkinter as tk
from tkinter import ttk
from datetime import datetime, timedelta
import subprocess
import threading
import time
import os
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.message import EmailMessage
import smtplib

# Paths to the parser scripts and executables
# These paths should be set according to your directory structure
# Note: Ensure that the paths are correct and accessible from the machine running this script
# You can change these paths to absolute paths if needed, but relative paths are used here for portability

FourSkelion_Historic_path = "./new_historic_parser/parser_new_hisoric"  # Path to the new historic parser script

FourSkelion_Live_path = "./new_live_parser/live_parser"  # Path to the new live parser script

Telemetry_path = "C:\\Program Files (x86)\\APP TELEMETRY\\Telemetry_Standalone.exe"        # Path to the Telemetry Client executable

Auto_sync_path = "C:\\Program Files (x86)\\FTP_SYNCH_APP\\FTP_AutoSynch.exe"   # Path to the Auto Sync executable

# This is the main application class that initializes the GUI and manages the scheduling of scripts
class SimpleSchedulerApp: # Override the class "SimpleSchedulerApp" with these functions. 
    
    def __init__(self, root):
        self.root = root
        self.root.title("NMS Files Parser")

        # Set the window icon
        self.set_icon("../assets/FASMETRICS_LOGONLY.ico")
        
        # Initialize Telemetry.exe
        self.telemetry_process = self.start_telemetry()
        
        self.script_mail_counter1 = 0
        self.script_mail_counter2 = 0
        self.script_mail_counter3 = 0
        self.script_mail_counter4 = 0
        
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
    def kill_self_after_timeout(thread, timeout=90000): # 90 seconds typically
        def monitor():
            time.sleep(timeout)
            if thread.is_alive():
                print(f"[Watchdog] Thread '{thread.name}' still alive after {timeout} sec. Exiting...")
                os.exit(1)
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
        
    # This function handles the closing of the application
    # It ensures that both Telemetry.exe and Auto_Sync.exe processes are terminated properly
    def on_closing(self):
        # Close Telemetry.exe if it is running
        if self.telemetry_process:
            try:
                self.telemetry_process.terminate()
                self.telemetry_process.wait()
                print(f"Closed Telemetry.exe with PID {self.telemetry_process.pid}")
            except Exception as e:
                print(f"Error closing Telemetry.exe: {e}")
             
        # Close Auto_Sync.exe if it is running   
        if self.auto_sync_process:
            try:
                self.auto_sync_process.terminate()
                self.auto_sync_process.wait()
                print(f"Closed Auto_Sync.exe with PID {self.auto_sync_process.pid}")
            except Exception as e:
                print(f"Error closing Auto_Sync.exe: {e}")
                
        self.root.destroy()
        print("Application closed successfully.")

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

    # This function runs a script in a separate thread
    # It handles the script name and starts the thread for execution
    # It also sets a timeout for the thread to ensure it does not run indefinitely
    def run_script_in_thread(self, script_name):
        try:
            print(script_name)
            # Create a thread for running the script
            thread = threading.Thread(target=self.run_script, args=(script_name,))
            thread.start()
            self.kill_self_after_timeout(self.thread, timeout=90000)    # 90 seconds typical timeout for each thread running the script
        except Exception as e:
            print(f"Error starting thread for {script_name}: {e}")
    
    # This function runs the specified script
    # It uses subprocess to execute the script and captures its output
    # It also handles the exit code and logs the output to files
    def run_script(self, script_name):
        try:
           
            # if script name is a method run it  
            if script_name == "Restart_telemetry":
                self.restart_telemetry() 
            # Run the external script
            else:
                result = subprocess.run(['python', f'{script_name}.py'], capture_output=True, text=True)
                
                # Based on the exit code print the output/error and save it in the correct logs
                if result.returncode == 0:
                    path = "../log_files/EXIT_CODE_0/"
                    print(result.stdout)
                    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") # Format the current date and time
                    # Create a file name with the current date and time
                    file_name = path  +f"{current_datetime}.txt"

                    # Open the file in write mode ('w')
                    with open(file_name, 'w') as file:
                        # Write the string to the file
                        file.write(result.stdout + '\n')
                else:
                    path = "../log_files/ERROR/"
                    print(result.stdout)
                    print("_______________________________________")
                    print(result.stderr)
                    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") # Format the current date and time
                    # Create a file name with the current date and time
                    file_name = path + f"{current_datetime}.txt"

                    # Open the file in write mode ('w')
                    with open(file_name, 'w') as file:
                        # Write the string to the file
                        file.write("----------ERROR MESSAGE:\n" + result.stderr)
                        file.write(result.stdout)

            # Update last run time
            current_time = datetime.now()
                        
            # Update UI labels and schedule next run
                                
            if script_name == FourSkelion_Historic_path:
                self.run_status_label31.config(text=f"Last Run: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                self.exit_code_label31.config(text=f"Exit Code: {result.returncode}")
                next_run_time = current_time + timedelta(minutes=180) # Schedule next run after 3 hours
                self.schedule_next_run_in_thread(script_name, self.run_status_label32, next_run_time) # Schedule next run in a separate thread
                # if this an error happens for the firs time
                if result.returncode != 0:
                    self.script_mail_counter3 += 1
                    if self.script_mail_counter3 == 1:
                        # send mail that there was an error with the parsers 
                        title = f"Error with: {script_name}"
                        body = f"There was an error with {script_name}\n"
                        self.email_report(title , body)  
                
            elif script_name == FourSkelion_Live_path:
                self.run_status_label41.config(text=f"Last Run: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                self.exit_code_label41.config(text=f"Exit Code: {result.returncode}")
                next_run_time = current_time + timedelta(minutes=2) # Schedule next run after 2 minutes
                self.schedule_next_run_in_thread(script_name, self.run_status_label42, next_run_time) # Schedule next run in a separate thread
                # if this an error happens for the firs time
                if result.returncode != 0:
                    self.script_mail_counter4 += 1
                    if self.script_mail_counter4 == 1:
                        # send mail that there was an error with the parsers 
                        title = f"Error with: {script_name}"
                        body = f"There was an error with {script_name}\n"
                        self.email_report(title , body)  
                
                
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

    # This function schedules the next run of a script
    # It updates the label with the next run time and sets a delay for the next execution
    def schedule_next_run(self, script_name, label, next_run_time):
        if label == self.run_status_label53:
            label.config(text=f"Next Telemetry Restart: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
            delay = int((next_run_time - datetime.now()).total_seconds() * 1000)  # Convert timedelta to milliseconds
            self.root.after(delay, lambda: self.run_script(script_name))
        else:
            label.config(text=f"Next Run: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
            delay = int((next_run_time - datetime.now()).total_seconds() * 1000)  # Convert timedelta to milliseconds
            self.root.after(delay, lambda: self.run_script(script_name))

    # This function sends an email report with the specified title and content
    # It uses the smtplib library to connect to an SMTP server and send the email
    def email_report(self, title, content):
        from_mail = 'drivetestteams@fasmetrics.com'
        to_mail = 'ioannis.astithas@fasmetrics.com,konstantinos.theodoropoulos@fasmetrics.com,dimitris.vasilas@fasmetrics.com,konstantinos.georgiafentis@fasmetrics.com'
        #server_user = '3skelion.fasmetrics@gmail.com'
        #server_user = 'rno@fasmetrics.com'
        server_user = 'drivetestteams@fasmetrics.com'
        #server_pass = '3skelion'
        #server_pass = 'rnaoteam1!'
        server_pass = 'drivetest1!'
        dproc = 'RGIS_DatParser.exe'

        #Connect to gmail account and send mail notification
        msg = EmailMessage()
        msg.set_content(content)
        msg['From'] = from_mail
        msg['To'] = to_mail
        msg['Subject'] = title
        #server = smtplib.SMTP('smtp.gmail.com', 587)
        server = smtplib.SMTP(host='smtp-mail.outlook.com', port=587)
        server.starttls()
        server.login(server_user, server_pass)
        server.send_message(msg)
        server.quit()

if __name__ == "__main__":
    root = tk.Tk()
    app = SimpleSchedulerApp(root)
    root.mainloop()