import tkinter as tk
from tkinter import ttk
from datetime import datetime, timedelta
import subprocess
import threading
import time
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.message import EmailMessage
import smtplib

# we aim to make all paths relative in order to run on any machine

TriSkelion_Parser_path = "./old_historic_parser/triskelionFTPParser"

Triskelion_Live_Parser_path = "./old_live_parser/triskelionFTPParser"

FourSkelion_Historic_path = "./new_historic_parser/parser_new_hisoric"

FourSkelion_Live_path = "./new_live_parser/live_parser"

Telemetry_path = "C:\Program Files (x86)\Telemetry Standalone\Telemetry_Standalone.exe"

Auto_sync_path = "C:\SONAR_SHARED_DATA\APP_FOLDER_2024\APP FTP AUTO SYNCH\FTP_AutoSynch.exe"


class SimpleSchedulerApp:
    
    def __init__(self, root):
        self.root = root
        self.root.title("3skelion - 4skelion NMS Files Parser")

        # Set the window icon
        self.set_icon("../assets/FASMETRICS_LOGONLY.ico")
        
        # Initialize Telemetry.exe
        self.telemetry_process = self.start_telemetry()
        
        self.script_mail_counter1 = 0
        self.script_mail_counter2 = 0
        self.script_mail_counter3 = 0
        self.script_mail_counter4 = 0
        
        self.auto_sync_process = self.start_auto_sync()
        
        # Bind the close event
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Create UI
        self.create_widgets()
    
    def set_icon(self, icon_path):
        if os.path.exists(icon_path):
            try:
                self.root.iconbitmap(icon_path)
            except Exception as e:
                print(f"Error setting icon: {e}")
        else:
            print(f"Icon file not found: {icon_path}")

    def start_telemetry(self):
        try:
            telemetry_process = subprocess.Popen([Telemetry_path])
            print(f"Started Telemetry.exe with PID {telemetry_process.pid}")
            return telemetry_process
        except Exception as e:
            print(f"Error starting Telemetry.exe: {e}")
            return None
    
    def start_auto_sync(self):
        try:
            auto_sync_process = subprocess.Popen([Auto_sync_path])
            print(f"Started Auto_sync.exe with PID {auto_sync_process.pid}")
            return auto_sync_process
        except Exception as e:
            print(f"Error starting Auto_sync.exe: {e}")
            return None

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

        # Script 1 Section
        frame1 = ttk.Frame(self.root, padding="20")
        frame1.grid(row=0, column=0, padx=20, pady=20)
        label1 = ttk.Label(frame1, text="3skelion Historic Data Parser", style="TLabel")
        label1.pack()
        button1 = ttk.Button(frame1, text="Run Now", command=lambda: self.run_script_in_thread(TriSkelion_Parser_path), style="Large.TButton")
        button1.pack()
        self.run_status_label11 = ttk.Label(frame1, text="Last Run: Never", style="Small.TLabel")
        self.run_status_label11.pack()
        self.exit_code_label11 = ttk.Label(frame1, text="Last Run Exit Code: N/A", style="Small.TLabel")
        self.exit_code_label11.pack()
        self.run_status_label12 = ttk.Label(frame1, text="Next Run: Not Scheduled", style="Small.TLabel")
        self.run_status_label12.pack()
        
         # Script 2 Section
        frame2 = ttk.Frame(self.root, padding="20")
        frame2.grid(row=0, column=1, padx=20, pady=20)
        label2 = ttk.Label(frame2, text="3skelion Live Data Parser", style="TLabel")
        label2.pack()
        button2 = ttk.Button(frame2, text="Run Now", command=lambda: self.run_script_in_thread(Triskelion_Live_Parser_path), style="Large.TButton")
        button2.pack()
        self.run_status_label21 = ttk.Label(frame2, text="Last Run: Never", style="Small.TLabel")
        self.run_status_label21.pack()
        self.exit_code_label21 = ttk.Label(frame2, text="Last Run Exit Code: N/A", style="Small.TLabel")
        self.exit_code_label21.pack()
        self.run_status_label22 = ttk.Label(frame2, text="Next Run: Not Scheduled", style="Small.TLabel")
        self.run_status_label22.pack()

        # Script 3 Section
        frame3 = ttk.Frame(self.root, padding="20")
        frame3.grid(row=0, column=2, padx=20, pady=20)
        label3 = ttk.Label(frame3, text="4skelion Historic Data Parser", style="TLabel")
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
        label4 = ttk.Label(frame4, text="4skelion Live Data Parser", style="TLabel")
        label4.pack()
        button4 = ttk.Button(frame4, text="Run Now", command=lambda: self.run_script_in_thread(FourSkelion_Live_path), style="Large.TButton")
        button4.pack()
        self.run_status_label41 = ttk.Label(frame4, text="Last Run: Never", style="Small.TLabel")
        self.run_status_label41.pack()
        self.exit_code_label41 = ttk.Label(frame4, text="Last Run Exit Code: N/A", style="Small.TLabel")
        self.exit_code_label41.pack()
        self.run_status_label42 = ttk.Label(frame4, text="Next Run: Not Scheduled", style="Small.TLabel")
        self.run_status_label42.pack()
        
        #Restart Telemetry Script Section
        button5 = ttk.Button(frame4, text="Restart Telemetry", command=lambda: self.run_script_in_thread("Restart_telemetry"), style="Large.TButton")
        button5.pack()
        self.run_status_label53 = ttk.Label(frame4, text="Next Telemetry Restart: Not Scheduled", style="Small.TLabel")
        self.run_status_label53.pack()

    def run_script_in_thread(self, script_name):
        try:
            print(script_name)
            # Create a thread for running the script
            thread = threading.Thread(target=self.run_script, args=(script_name,))
            thread.start()
        except Exception as e:
            print(f"Error starting thread for {script_name}: {e}")
    
    def run_script(self, script_name):
        try:
           
            # if script name is a method run it  
            if script_name == "Restart_telemetry":
                self.restart_telemetry() 
            # Run the external script
            else:
                result = subprocess.run(['python', f'{script_name}.py'], capture_output=True, text=True)
                
                #   Based on the exit code print the output/error and save it in the correct logs
                if result.returncode == 0:
                    path = "../log_files/EXIT_CODE_0/"
                    print(result.stdout)
                    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
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
                    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                    file_name = path + f"{current_datetime}.txt"

                    # Open the file in write mode ('w')
                    with open(file_name, 'w') as file:
                        # Write the string to the file
                        file.write("----------ERROR MESSAGE:\n" + result.stderr)
                        file.write(result.stdout)

            # Update last run time
            current_time = datetime.now()

            # Update UI labels and schedule next run
            if script_name == TriSkelion_Parser_path:
                self.run_status_label11.config(text=f"Last Run: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                self.exit_code_label11.config(text=f"Exit Code: {result.returncode}")
                next_run_time = current_time + timedelta(minutes=180)
                self.schedule_next_run_in_thread(script_name, self.run_status_label12, next_run_time)
                # if this an error happens for the firs time
                if result.returncode != 0:
                    self.script_mail_counter1 += 1
                    if self.script_mail_counter1 == 1:
                        # send mail that there was an error with the parsers 
                        title = f"Error with: {script_name}"
                        body = f"There was an error with {script_name}\n"
                        self.email_report(title , body)
                        
            # Update UI labels and schedule next run
            if script_name == Triskelion_Live_Parser_path:
                self.run_status_label21.config(text=f"Last Run: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                self.exit_code_label21.config(text=f"Exit Code: {result.returncode}")
                next_run_time = current_time + timedelta(minutes=180)
                self.schedule_next_run_in_thread(script_name, self.run_status_label22, next_run_time)
                # if this an error happens for the firs time
                if result.returncode != 0:
                    self.script_mail_counter2 += 1
                    if self.script_mail_counter2 == 1:
                        # send mail that there was an error with the parsers 
                        title = f"Error with: {script_name}"
                        body = f"There was an error with {script_name}\n"
                        self.email_report(title , body)  
                                
            elif script_name == FourSkelion_Historic_path:
                self.run_status_label31.config(text=f"Last Run: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                self.exit_code_label31.config(text=f"Exit Code: {result.returncode}")
                next_run_time = current_time + timedelta(minutes=180)
                self.schedule_next_run_in_thread(script_name, self.run_status_label32, next_run_time)
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
                next_run_time = current_time + timedelta(minutes=2)
                self.schedule_next_run_in_thread(script_name, self.run_status_label42, next_run_time)
                # if this an error happens for the firs time
                if result.returncode != 0:
                    self.script_mail_counter4 += 1
                    if self.script_mail_counter4 == 1:
                        # send mail that there was an error with the parsers 
                        title = f"Error with: {script_name}"
                        body = f"There was an error with {script_name}\n"
                        self.email_report(title , body)  
                
                
            elif script_name == "Restart_telemetry":
                next_run_time = current_time + timedelta(minutes=60)
                self.schedule_next_run_in_thread(script_name, self.run_status_label53, next_run_time)

        except Exception as e:
            print(f"An error occurred: {e}")
            
    def schedule_next_run_in_thread(self, script_name, label, next_run_time):
        try:
            # Create a thread for scheduling next run
            thread = threading.Thread(target=self.schedule_next_run, args=(script_name, label, next_run_time))
            thread.start()
        except Exception as e:
            print(f"Error starting scheduling thread for {script_name}: {e}")

    def schedule_next_run(self, script_name, label, next_run_time):
        if label == self.run_status_label53:
            label.config(text=f"Next Telemetry Restart: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
            delay = int((next_run_time - datetime.now()).total_seconds() * 1000)  # Convert timedelta to milliseconds
            self.root.after(delay, lambda: self.run_script(script_name))
        else:
            label.config(text=f"Next Run: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
            delay = int((next_run_time - datetime.now()).total_seconds() * 1000)  # Convert timedelta to milliseconds
            self.root.after(delay, lambda: self.run_script(script_name))

    def email_report(self, title, content):
        from_mail = 'drivetestteams@fasmetrics.com'
        to_mail = 'manos.papadakakis@fasmetrics.com,konstantinos.theodoropoulos@fasmetrics.com,dimitris.vasilas@fasmetrics.com,konstantinos.georgiafentis@fasmetrics.com'
        #server_user = '3skelion.fasmetrics@gmail.com'
        #server_user = 'rno@fasmetrics.com'
        server_user = 'drivetestteams@fasmetrics.com'
        #server_pass = '3skelion'
        #server_pass = 'rnaoteam1!'
        server_pass = 'drivetest1!'
        dproc = 'RGIS_DatParser.exe'

        #Connect to gmail account and sent mail notification
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