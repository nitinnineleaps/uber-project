def uber_project():
    import sys
    from pprint import pprint

    # list down all the files all we required
    import os
    for dirname, _, filenames in os.walk('/usr/local/airflow/ip_files/archive'):
        for filename in filenames:
            print(os.path.join(dirname, filename))

    import pandas as pd
    import numpy as np
    files = [filename for filename in os.listdir(r'/usr/local/airflow/ip_files/archive') if filename.startswith("uber-")]
    print(files)

    # Removing the files that we do not required
    files.remove('uber-raw-data-janjune-15.csv')

    # concateing the data
    path = r'/usr/local/airflow/ip_files/archive'

    Data = pd.DataFrame()

    for file in files:
        df = pd.read_csv(path + "/" + file, encoding='utf-8')
        Data = pd.concat([df, Data])

    # looking at the sample data
    print(Data.sample(frac=0.6))

    # checking data attributes
    print(Data.shape)

    data = Data.copy()
    print(data.dtypes)

    # Data Preprocessing
    data['Date/Time'] = pd.to_datetime(data['Date/Time'], format='%m/%d/%Y %H:%M:%S')
    print(data.dtypes)

    data['month'] = data['Date/Time'].dt.month
    data['weekday'] = data['Date/Time'].dt.day_name()
    data['day'] = data['Date/Time'].dt.day
    data['hour'] = data['Date/Time'].dt.hour
    data['minute'] = data['Date/Time'].dt.minute
    print(data.head(30))
    print(data.dtypes)
    print(type(data))

    # writing data to csv file
    data.to_csv("data.csv", "w")
    """
    import pandas as pd
    import pandasql as ps
    df = pd.DataFrame(data)
    ps.sqldf("select Lat, Lon, count([Date/Time]) as Number_of_Trips from data group by Lat,Lon limit 10;")
    """
    # which month and which day of that month, marks the highest demand of UBER trips?
    weekday = pd.DataFrame(data[['day', 'month']].value_counts()).reset_index()
    weekday.columns = ['Day', 'Month', 'Count']
    weekday['Day'] = pd.Categorical(weekday['Day'],
                                    categories=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                                                21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31], ordered=True)
    weekday['Month'] = pd.Categorical(weekday['Month'], categories=[4, 5, 6, 7, 8, 9], ordered=True)
    print(weekday)

    # writing weekday to csv
    weekday.to_csv("weekday.csv")
    """
    # which hour is busiest in the day for Uber?
    import pandas as pd
    import pandasql as ps
    ps.sqldf("select hour, count(hour) as Hour from data group by hour;")
    
    # Distribution in trips shown by days in a month
    ps.sqldf("select day, count(day) as Number_of_Trips from data group by day;")

    # Base locations recording highest number of pick-ups
    ps.sqldf("select Base, hour ,count(Base) as Number_of_Trips from data group by hour, Base order by count(Base) desc;")

    # Cross Analysis done between hours and weekdays
    df2 = ps.sqldf("select weekday,hour, count([Date/Time]) as Number_of_trips from data group by weekday, hour;")
    print(df2)

    df4 = df2.pivot(index='weekday', columns='hour')
    print(df4)

    df2.unstack()
    """
