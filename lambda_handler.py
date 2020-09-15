import pandas as pd
import smtplib
import os

def load_data(window_length, state='Massachusetts'):
	covid_data = pd.read_csv('https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv')
	covid_data['date'] = pd.to_datetime(covid_data['date'])
	covid_data.sort_values('date', inplace=True)
	cutoff_date = pd.to_datetime('today') - pd.DateOffset(days=window_length+1)
	covid_data = covid_data.loc[covid_data['date'] >= cutoff_date]
	state_data = covid_data.loc[covid_data['state'] == state]
	return state_data
	
def get_moving_average(data):
	data['yesterday_cases'] = data['cases'].shift()
	data['new_cases'] = data['cases'] - data['yesterday_cases']
	return round(data['new_cases'].mean())
	
def send_email(window_length, state_average, county_average, email):
	sending_address = 'covid.emails20@gmail.com'
	password = os.environ['email_password']
	server = smtplib.SMTP('smtp.gmail.com', 587)
	server.ehlo()
	server.starttls()
	server.ehlo()
	server.login(sending_address, password)
	
	subject = "Change in Covid Cases"
	body = "Over the past {} days the average county cases were {} and the average state cases were {}.".\
				format(window_length, county_average, state_average)
	email_text = 'Subject: {}\n\n{}'.format(subject, body)
	server.sendmail(sending_address, email, email_text)
	server.close()
	
	
def lambda_handler(event, context):
    window_length = 7
    covid_data = load_data(window_length)
    statewide_data = covid_data.groupby('date')['cases'].sum().reset_index()
    state_average = get_moving_average(statewide_data)
    county_data = covid_data.loc[covid_data['county'] == 'Essex']
    county_average = get_moving_average(county_data)
    email_address = os.environ['target_email']
    send_email(window_length, state_average, county_average, email_address)