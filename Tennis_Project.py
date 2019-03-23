from tkinter import *
from Tennis_Functions import Tennis_Downloads


database = Tennis_Downloads("tennis.db")

class Window(object):

	def __init__(self,window):

		self.window = window

		self.window.wm_title("Master Download")

		l1 = Label(window,text = "Start Date:")
		l1.grid(row=0,column=0)

		l2=Label(window,text="End Date:")
		l2.grid(row=1,column=0)

		l3=Label(window,text="Number Days:")
		l3.grid(row=2,column=0)

		l3=Label(window,text="Version Number:")
		l3.grid(row=3,column=0)

		self.start_date=StringVar()
		self.e1=Entry(window,textvariable=self.start_date)
		self.e1.grid(row=0,column=1)

		self.end_date=StringVar()
		self.e2=Entry(window,textvariable=self.end_date)
		self.e2.grid(row=1,column=1)

		self.num_days=StringVar()
		self.e3=Entry(window, textvariable=self.num_days)
		self.e3.grid(row=2,column=1)

		self.num_version=StringVar()
		self.e4=Entry(window,textvariable=self.num_version)
		self.e4.grid(row=3,column=1)

		b1=Button(window,text="Download Links Odds", width=18, command=self.Run_Download_Links_Odds)
		b1.grid(row=0,column=3)

		b2=Button(window,text="Download Links Reg", width=18, command=self.Run_Download_Links_Reg)
		b2.grid(row=0,column=5)

		b3=Button(window,text="Download Odds", width=18, command=self.Run_Download_Odds)
		b3.grid(row=1,column=3)

		b5=Button(window,text="Download Reg", width=18, command=self.Run_Download_Reg)
		b5.grid(row=1,column=5)		

		b8=Button(window,text="Download Rank", width=18, command=self.Run_Download_Rank)
		b8.grid(row=2,column=3)		
        
		b9=Button(window,text="Update Players", width=18, command=self.Run_Update_Players)
		b9.grid(row=2,column=5)		        

		b7=Button(window,text="Update IDs", width=18, command=self.Run_Update_IDs)
		b7.grid(row=3,column=5)	

		b4=Button(window,text="Map Output", width=18, command=self.Run_Map_Out)
		b4.grid(row=3,column=3)

		b6=Button(window,text="Close", width=18,command=window.destroy)
		b6.grid(row=4,column=3)



	def Run_Download_Links_Odds(self):
		if self.end_date.get() == '':
			EndDt = 0
		else:
			EndDt = self.end_date.get()
		if self.num_days.get() == '':
			NumDs = 0
		else:
			NumDs = self.num_days.get()
		Tennis_Downloads.Download_Links_Odds(self, self.start_date.get(), EndDt, float(NumDs), "tennis.db")

	def Run_Download_Links_Reg(self):
		if self.end_date.get() == '':
			EndDt = 0
		else:
			EndDt = self.end_date.get()
		if self.num_days.get() == '':
			NumDs = 0
		else:
			NumDs = self.num_days.get()
		Tennis_Downloads.Download_Links_Reg(self, self.start_date.get(), EndDt, float(NumDs), "tennis.db")

	def Run_Download_Reg(self):
		if self.end_date.get() == '':
			EndDt = 0
		else:
			EndDt = self.end_date.get()
		if self.num_days.get() == '':
			NumDs = 0
		else:
			NumDs = self.num_days.get()
		Tennis_Downloads.Download_Reg(self, self.start_date.get(), EndDt, float(NumDs), "tennis.db")	

	def Run_Download_Odds(self):
		if self.end_date.get() == '':
			EndDt = 0
		else:
			EndDt = self.end_date.get()
		if self.num_days.get() == '':
			NumDs = 0
		else:
			NumDs = self.num_days.get()
		Tennis_Downloads.Download_Odds(self, self.start_date.get(), EndDt, float(NumDs), "tennis.db")	

	def Run_Download_Rank(self):
		if self.end_date.get() == '':
			EndDt = 0
		else:
			EndDt = self.end_date.get()
		if self.num_days.get() == '':
			NumDs = 0
		else:
			NumDs = self.num_days.get()
			Tennis_Downloads.Ranking_Down(self, self.start_date.get(), EndDt, float(NumDs), "tennis.db")


	def Run_Update_Players(self):
		Tennis_Downloads.Update_Players(self, "tennis.db")
        
	def Run_Update_IDs(self):
		Tennis_Downloads.Update_IDs(self, "tennis.db")
        
	def Run_Map_Out(self):
		Tennis_Downloads.Map_Out(self, "tennis.db")


window = Tk()
Window(window)
window.mainloop()