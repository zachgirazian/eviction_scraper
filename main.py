if True:
    import pandas as pd
    import numpy as np
    import requests
    from bs4 import BeautifulSoup
    import os
    from astropy.time import Time
    from time import sleep
    import re
    from random import randint
    import datetime
    
# Initial login using Napier and extract login cookie
def login(username,password):

    # Login
    reader = Reader(Opener())
    reader.init()
    result = reader.login(username, password)#.decode('UTF-8')

    if 'A user is already logged on under this account' in str(result):
        print('\n \n \n Login Failed: A user is already logged on under this account \n \n ')
    
    # Extract cookie
    thecookie = reader.opener.get_cookies()
    thecookiestr = str(thecookie)
    finalCookie =  thecookiestr.split('node')[0].split('x8c&')[-1] + 'node' + thecookiestr.split('node')[1][0]

    cookies = finalCookie
           
    return reader, cookies

# set headers for county search
def set_headers(countyID):    
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        'origin':blank,
        'referer': blank,
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    }
    return headers

# set data for request - input county id and county num
def set_data(countyID,fromDate,toDate):
    data = [
        ('last', ''),
        ('first', ''),
        ('fromDate', fromDate),
        ('toDate', toDate),
        ('role', 'ALL'),
    ]

    return data

# Search for FED cases list for specific county and date range
def request(cookie,countyID,startDate,endDate):
    
    response = requests.post(
        'blank',
        cookies=cookie,
        headers=set_headers(countyID),
        data=set_data(countyID,startDate,endDate),
    )
    
    # get soup
    soup = BeautifulSoup(response.text,'lxml')
    
    return soup

# Extract relevant data (case #, date, parties, etc.) and store in arrays
def extract_cases(thesoup,dict,cookie,countyID,startDate,endDate):

    rows = thesoup.findAll('tr')
    
    # ZG May 5th, 2024: The error doesn't say "has more than 100 records" anymore it just says:
    # Application Error.  Please retry your last action again later.
    # If the problem persists, please contact the help desk @ 1-800-831-1396 or 
    # Solution - always have polk county be a double search

    if 'more than "100" records' in rows[0].text:
        print('\n' + countyName + ' has more than 100 records - requery twice with smaller date ranges \n')
        stop()
        # 1st query: fromDate to fromDate + 7 days
        fromDateQ1 = fromDate
        toDateQ1 = (Time.strptime(fromDate,'%m/%d/%Y').datetime + datetime.timedelta(days=7)).strftime('%m/%d/%Y')
           
        # new request, date range 1
        soup = request(cookies,county_id,fromDateQ1,toDateQ1)
        
        # extract cases, date range 1
        dict = extract_cases(soup,dict,cookie,countyID,fromDateQ1,toDateQ1)
        
        # 2nd query: (fromDate + 7 days) to (fromDate + 7 days + 1 year)
        fromDateQ2 = toDateQ1
        toDateQ2 = (Time.strptime(fromDateQ2,'%m/%d/%Y').datetime + datetime.timedelta(days=365)).strftime('%m/%d/%Y')
        
        # new request, date range 2
        soup = request(cookies,county_id,fromDateQ2,toDateQ2)
        
        # extract cases, date range 2
        dict = extract_cases(soup,dict,cookie,countyID,fromDateQ2,toDateQ2)

        return dict
    
    # loop through each case entry and store in dictionary
    else:
        for j, col1 in enumerate(rows):
            if j == 0: continue # skip first entry (headers)
            if j > (len(rows) - 3): break # skip last two rows (no case info)
            
            cases1 = col1.findAll('td')
            dict['county'].append(countyName)
            dict['case'].append(cases1[0].text.replace(u'\xa0',u' ').strip())
            dict['parties'].append(cases1[1].text.lower().strip())
            if 'FORCIBLE ENTRY' not in cases1[2].text.strip(): #just a check to make sure the website is actually showing FED cases and not other cases
                print('\n \n NOT FORCIBLE ENTRY CASE!! \n\n')
                stop()
            dict['date'].append(Time.strptime(cases1[3].text.strip(),'%m/%d/%Y %I:%M %p').datetime)

        return dict


# Zach Girazian
# This is used to scrape FED data for weekly FED pulls. Some NAPIER functions are used to login, the login cookie is manually saved, then each county is looped through and searched
# new files created are master_YYYYMM
###########################################################################################################
# ** DIRECTIONS ** #
###########################################################################################################

# 1. Change todays_date to today's date (friday for friday pull)
todays_date = 'YYYYMMDD'
last_pull_date = 'YYYYMMDD'

# 2. Choose if want to scrape or not. Once all data scraped can trun scraping off to just work on updating spreadsheet
DO_SCRAPE = False

# 3. Go on sharepoint, download the master spreadhseet, run fed_scrape_sharepoint. Save the new file as the same filename and reupload to sharepoint.


###########################################################################################################
# Set directory and date range for this week's pull, load county list and id's
# set file name of current spreadsheet that will be updated
# folder names and such are automated based on today's date and assuming it's a weekly Friday pull
###########################################################################################################
todays_date_dt = datetime.datetime.strptime(todays_date,'%Y%m%d')

# This directory name and folder name automation only works if doing weekly Friday pulls!! **
# Directory for today's scraping
#check if directory exists, if not, make it
dir = 'put_directory_here' + todays_date + '/'
if not os.path.isdir(dir):
    os.mkdir(dir)

# old spread sheet filename - this is in the folder from last week
old_filename = 'put_directory_here' + last_pull_date + '/master_' + last_pull_date + '.csv'

# Date range
fromDate = (todays_date_dt + datetime.timedelta(days=-10)).strftime('%m/%d/%Y') # today's date minus 7 days
toDate =   (todays_date_dt + datetime.timedelta(days=365)).strftime('%m/%d/%Y') #just add a year

# load countyIDs list
AllcountyList = pd.read_pickle('counties_list_dataFrame.pkl')


###########################################################################################################
# Start Scrape
###########################################################################################################
if DO_SCRAPE:
    ###########################################################################################################
    # If crashes midscrape can use this to start at specific county and don't have to rescrape everything again
    ###########################################################################################################
    # set to True if restarting midscrape because of crash
    restart = False
    lastGoodCounty = 'pocahontas' # out the name of the last county where the scrape was successfull

    # set up for restart or not restart - load dictionary and set new county to start at
    if restart:
        print('\n \n Restarting scrape \n last good county: ' + lastGoodCounty)
        
        # load dictionary with results before crash
        aa =  np.load(dir + 'temp_dict2.npy',allow_pickle=True)
        dict = {'county':aa.item().get('county'),'case':aa.item().get('case'),'parties':aa.item().get('parties'),'tenant':[],'date':aa.item().get('date')}
        
        # if the dictionary had the county where it crashed then remove it (dictionary should only have successfully scraped counties)
        inds = np.where(np.array(dict['county']) == lastGoodCounty)[0]
        for key in dict:
            dict[key] = dict[key][0:inds[-1]+1]
            
        # Trim county list to remove all counties that have already been scraped
        countyList = AllcountyList[AllcountyList.county_name[AllcountyList.county_name == lastGoodCounty].index.tolist()[0]+1:]
        
        print('\n \n Scraping these counties: \n')
        print(countyList)
        
    else:
        # make dictionary to hold new FEDs
        dict = {'county':[],'case':[],'parties':[],'tenant':[],'date':[]}
        countyList = AllcountyList # scrape all counties


    # debug test a single county
    # countyList = countyList[countyList['county_name']=='polk']



    ############
    # Scrape
    ############

    # Login to website and extract cookie (uses Napier)
    username='username'
    password = 'password'
    reader, cookies = login(username,password)

    # Search and parse cases from each county using request - loop trhough each county
    for ind in countyList.index:
        
        # Get county name and ids
        countyName = countyList['county_name'][ind]
        county_id = countyList['countyid'][ind]
        print(countyName,county_id)
        
        ######################################################################################################
        # polk county often has so many cases that a entire year query results in error. For Polk do
        # two querys with dates split
        # ZG May 5th, 2024: The error doesn't say "has more than 100 records" anymore it just says:
        # Application Error.  Please retry your last action again later.
        # If the problem persists, please contact 
        # Solution - always have polk county be a double search
        ######################################################################################################
        if county_id == '05771': # Polk county
        
            # 1st query: fromDate to fromDate + 7 days
            fromDateQ1 = fromDate
            toDateQ1 = (Time.strptime(fromDate,'%m/%d/%Y').datetime + datetime.timedelta(days=7)).strftime('%m/%d/%Y')
            
            # new request, date range 1
            soup = request(cookies,county_id,fromDateQ1,toDateQ1)
            
            # extract cases, date range 1
            dict = extract_cases(soup,dict,cookies,county_id,fromDateQ1,toDateQ1)
            
            # 2nd query: (fromDate + 7 days) to (fromDate + 7 days + 1 year)
            fromDateQ2 = toDateQ1
            toDateQ2 = (Time.strptime(fromDateQ2,'%m/%d/%Y').datetime + datetime.timedelta(days=365)).strftime('%m/%d/%Y')
            
            # new request, date range 2
            soup = request(cookies,county_id,fromDateQ2,toDateQ2)
            
            # extract cases, date range 2
            dict = extract_cases(soup,dict,cookies,county_id,fromDateQ2,toDateQ2)
            
            #count = 0
            #for ll in range(len(dict['county'])):
            #    if dict['county'][ll] == 'polk':
            #        print(dict['case'][ll])
            #        count = count+1
            
        else: # not Polk county
        
            # Search for FED cases in specific county and date range and get soup 
            soup = request(cookies,county_id,fromDate,toDate)

            # Check if there are no records - if so, don't store just print 
            if 'No records were found matching your search criteria' in soup.text:
                #dict['county'].append(countyName)
                #dict['case'].append(-1)
                #dict['parties'].append(-1)
                #dict['date'].append(-1)
                print('\n \n No cases found for ' + countyName + ' county \n \n')
                continue
                
            # Extract relevant data (case #, date, parties, etc.) and store in arrays
            dict = extract_cases(soup,dict,cookies,county_id,fromDate,toDate)
        
        # Pause to not overwhelm website
        sleepTime = randint(5,15)
        print('\n \n' + countyName)
        print('\n Sleeping for ' + str(sleepTime) + ' seconds')
        sleep(sleepTime)
        
        # temperarily save progress
        np.save(dir + 'temp_dict2', dict)
        
        # Early break for debugging
        #if ind > 3: break

    # Logoff
    reader.logoff()


    ##############################################################################################
    # Scraping is over, time to add tenant name and save as pandas
    ##############################################################################################
    # Fill in tenant array by extrtacting tenant name from parties
    nameTemp = dict['parties']
    name = [str(n1).lower() for n1 in nameTemp]
    #name = np.char.lower(np.array(str(dict['parties']))) # make lowercase
    delims = ' v ', ' vs ', ' v. ', ' vs. ' # sometimes this picks up a middle initial that s "V"
    for name1 in name:
        # flexible string search to split tenant and landlord
        regex_pattern = '|'.join(map(re.escape, delims))
        dict['tenant'].append(re.split(regex_pattern, name1, 0)[-1])    

    # Make pandas, sort
    df = pd.DataFrame(dict)
    df = df.sort_values('county')
    df = df.reset_index()

    # Save newly scraped cases
    df.to_pickle(dir + 'new_cases.pkl')
    stop()



# if already scraped, load scraped dataframe
if not DO_SCRAPE:
    df = pd.read_pickle(dir + 'new_cases.pkl')
    df = df.sort_values('county')
    df = df.reset_index(drop=True)


# count number of new entries found this week
dfCounts = df.groupby('county').count()['case']
print(dfCounts.to_string())


##############################################################################################
# Load previous week's master spreadsheet, add new entries by checking for duplicate case #'s 
# and keep most recent hearing date
##############################################################################################

# load sheet updated through last week's pull
dfold = pd.read_csv(old_filename)
dfold = dfold.loc[:, ~dfold.columns.str.contains('^Unnamed')] #remove unamed rows
dfold['update'] = pd.Series() # to store notes on if date updated or if new case for this week and remove last weeks note


# Remove second space that appears in case # string in scraped cases - also do for master df too just in case (match based on case # below so needed)
df['case'] = df['case'].str.split(' ',n=1).str[0].str.strip() + ' ' + df['case'].str.split(' ',n=1).str[-1].str.strip()
dfold['case'] = dfold['case'].str.split(' ',n=1).str[0].str.strip() + ' ' +  dfold['case'].str.split(' ',n=1).str[-1].str.strip()


# make copy for appending new cases just scraped
dfnew = dfold.copy(deep=True)


# df = newly scraped cases
# dfold = spreadsheet from last week
# dfnew = dfold but updating with new entries - ends up beng final product

# loop though each case just pulled from scrape, check if in old sheet. If not, add it. If is, keep latest date
for index, row in df.iterrows():
    
    # reset flags
    new_case_to_add = False
    already_added_to_new_sheet = False
    
    # check if duplicate case number - was the case number in the old sheet?
    dfcheck = dfold[dfold['case'].str.contains(row['case'].split('SCSC')[-1])] # in old sheet?
    dfcheck2 = dfnew[dfnew['case'].str.contains(row['case'].split('SCSC')[-1])] # in this week's sheet?
    #dfcheck = dfold[dfold['case'] == row['case']] #case in old sheet? # these dont work 
    #dfcheck2 = dfnew[dfnew['case'] == row['case']] #case in new sheet? # not working delete
    
    # if not in old sheet or in new sheet needs to be added to new sheet as new case entry
    if dfcheck.size == 0:      # not in old sheet
        if dfcheck2.size == 0: # not in new sheet either so add case
            dfnew = pd.concat([dfnew,df.iloc[[index]]],ignore_index=True)
            dfnew.loc[dfnew['case'] == df['case'][index],'update'] = 'NEW_ENTRY'
            print('NEW CASE')
    else: #if duplicate, keep most recent date
        # extract dates and compare
        old = dfcheck['date'].to_list()[0]
        oldDate = datetime.datetime.strptime(old,'%Y-%m-%d %H:%M:%S')
        newDate =  row['date'].to_pydatetime()
        
        # if same date and time then can move on without doing anything
        # if not, then just edit the date to the most recent
        if oldDate != newDate:
            if newDate > oldDate:
                print('UPDATING DATE')
                # update new date because it is more recent and add note
                dfnew.loc[dfnew['case'] == df['case'][index],'date'] = newDate
                dfnew.loc[dfnew['case'] == df['case'][index],'update'] = 'NEW_DATE'
        else:
            print('DUPLICATE W/ SAME DATE - SKIPPING')
    

# reset dfnew index and sort by county
dfnew['county'] = dfnew['county'].str.lower() # lower case counties
dfnew = dfnew.sort_values('county',ignore_index=True)
dfnew['update'].fillna(' ',inplace=True) # make NaN updates blank spaces
dfnew = dfnew.reset_index(drop=True)
#dfnew =dfnew.sort_values(by='county', key=lambda col: col.str.lower())
dfnew = dfnew.drop(columns=['index'])

# save new sheet master sheet wth all 2024 pulls
dfnew.to_csv(dir + 'master_' + todays_date + '.csv',na_rep='NULL')

# make sheet to fill in the # of new cases for each county from this week's pull
# extract only the newly pulled cases
df_new_cases = dfnew[dfnew['update'] == 'NEW_ENTRY'] 

# group them and count them per county
df_new_cases_g = df_new_cases.groupby('county').count()

# merge with all counties list
dfcounty_count = pd.merge(AllcountyList,df_new_cases_g,right_on='county',left_on='county_name',how='left')

# fill in NaNs as zeros
dfcounty_count['case'].fillna(0,inplace=True)

# extract the two columns we need
dfcounty_count = dfcounty_count[['county_name','case']] 

# rename count column appropriately
dfcounty_count.rename(columns={'case':'new_cases_count'}, inplace=True)

# Need to combine the two Lee counties into one row and delete the leee (south) and lee (north) rows
clee = [np.sum(dfcounty_count['new_cases_count'][dfcounty_count['county_name'] == 'lee (north)'].to_list() + dfcounty_count['new_cases_count'][dfcounty_count['county_name'] == 'lee (south)'].to_list())]
dflee = pd.DataFrame({'county_name':'lee','new_cases_count':clee })
dfcounty_count = pd.concat([dfcounty_count,dflee],ignore_index=True).fillna(0)

# delete lee south and north
dfcounty_count = dfcounty_count[((dfcounty_count.county_name != 'lee (north)') & (dfcounty_count.county_name != 'lee (south)') )]

# resort by county name alphabetical
dfcounty_count = dfcounty_count.sort_values('county_name',ignore_index=True)

# save county count sheet
dfcounty_count.to_csv(dir + 'county_count_' + todays_date + '.csv')
print(dfcounty_count.to_string())


