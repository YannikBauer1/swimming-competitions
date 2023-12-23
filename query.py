import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np
import math
import psycopg2

conn = psycopg2.connect("dbname=datawarehouse user=yannik")
cursor_psql = conn.cursor()

def query1():
    sql = "select stroke, athlete.gender as gender, \
                   avg(swimtime) as avg_time \
            from fact, athlete, swimstyle \
            where fact.swimstylecode = swimstyle.swimstylecode and fact.license = athlete.license\
            group by stroke,gender \
            order by stroke,gender"
    cursor_psql.execute(sql)
    results = cursor_psql.fetchall()

    barWidth = 0.25
    fem = [i[2] for i in results if i[1]=="F"]
    fem = [float(str(i.seconds)+"."+str(i.microseconds)) for i in fem]
    mas = [i[2] for i in results if i[1]=="M"]
    mas = [float(str(i.seconds)+"."+str(i.microseconds)) for i in mas]
    br1 = np.arange(len(fem))
    br2 = [x + barWidth for x in br1]
    plt.bar(br1, fem, color='r', width=barWidth, edgecolor='grey', label='fem')
    plt.bar(br2, mas, color='b', width=barWidth, edgecolor='grey', label='mas')
    plt.ylabel('Time in sec.microsec', fontweight='bold', fontsize=15)
    plt.xticks([r + barWidth for r in range(len(fem))],[results[i][0] for i in range(0,len(results),2)])
    plt.legend()
    plt.title("AVG swimtime for gender and stroke")
    plt.show()
query1()

def query2():
    sql = "select course, stroke, \
                   avg(swimtime) as avg_time \
            from fact, meet, swimstyle \
            where fact.swimstylecode = swimstyle.swimstylecode and fact.meetid = meet.meetid\
            group by course,stroke \
            order by stroke,course"
    cursor_psql.execute(sql)
    results = cursor_psql.fetchall()
    for res in results:
        print(res)

    barWidth = 0.25
    LCM = [i[2] for i in results if i[0]=="LCM"]
    LCM = [float(str(i.seconds)+"."+str(i.microseconds)) for i in LCM]
    SCM = [i[2] for i in results if i[0]=="SCM"]
    SCM = [float(str(i.seconds)+"."+str(i.microseconds)) for i in SCM]
    br1 = np.arange(len(LCM))
    br2 = [x + barWidth for x in br1]
    plt.bar(br1, LCM, color='r', width=barWidth, edgecolor='grey', label='LCM')
    plt.bar(br2, SCM, color='b', width=barWidth, edgecolor='grey', label='SCM')
    plt.ylabel('Time in sec.microsec', fontweight='bold', fontsize=15)
    plt.xticks([r + barWidth for r in range(len(LCM))],[results[i][1] for i in range(0,len(results),2)])
    plt.legend()
    plt.title("AVG swimtime for course and stroke")
    plt.show()
query2()

def query3():
    sql = "select course, stroke, \
                   min(swimtime) as avg_time \
            from fact, meet, swimstyle \
            where fact.swimstylecode = swimstyle.swimstylecode and fact.meetid = meet.meetid\
            group by course,stroke \
            order by stroke,course"
    cursor_psql.execute(sql)
    results = cursor_psql.fetchall()

    barWidth = 0.25
    LCM = [i[2] for i in results if i[0]=="LCM"]
    LCM = [float(str(i.second)+"."+str(i.microsecond)) for i in LCM]
    SCM = [i[2] for i in results if i[0]=="SCM"]
    SCM = [float(str(i.second)+"."+str(i.microsecond)) for i in SCM]
    br1 = np.arange(len(LCM))
    br2 = [x + barWidth for x in br1]
    plt.bar(br1, LCM, color='r', width=barWidth, edgecolor='grey', label='LCM')
    plt.bar(br2, SCM, color='b', width=barWidth, edgecolor='grey', label='SCM')
    plt.ylabel('Time in sec.microsec', fontweight='bold', fontsize=15)
    plt.xticks([r + barWidth for r in range(len(LCM))],[results[i][1] for i in range(0,len(results),2)])
    plt.legend()
    plt.title("MIN swimtime for course and stroke")
    plt.show()
query3()

def query4():
    sql = "select * \
            from ( \
			select meet.name as meet_name, \
	        club.name as club_name, \
	        sum(fact.points) as total_points,\
	        row_number() over (partition by meet.name order by sum(fact.points) desc) as rn \
            from fact, meet, club \
            where fact.meetid = meet.meetid and fact.code = club.code \
            group by club_name, meet.name \
            order by meet_name, total_points desc, rn \
            ) as r \
            where r.rn <= 5\
            order by r.meet_name"
    cursor_psql.execute(sql)
    results = cursor_psql.fetchall()
    for res in results:
        print(res)

    meets = [i[0] for i in results]
    meets = list(dict.fromkeys(meets))
    for meet in meets:
        clubs = [i[1] for i in results if i[0]==meet]
        points = [i[2] for i in results if i[0]==meet]
        print(clubs)
        print(points)
        plt.figure(figsize=(9, 5))
        plt.barh(clubs, points)
        plt.title('Top 5 clubs in '+meet)
        plt.xlabel('Points')
        plt.subplots_adjust(left=0.4, right=0.8, top=0.9, bottom=0.1)
        plt.show()
query4()

def query5():
    sql = "select gender, agegroup.agegroupid, \
                       avg(swimtime) \
                from fact, agegroup, athlete \
                where fact.agegroupid = agegroup.agegroupid and athlete.license = fact.license\
                group by agegroup.agegroupid, gender \
                order by gender, agegroupid"
    cursor_psql.execute(sql)
    results = cursor_psql.fetchall()

    fem = [i[2] for i in results if i[0] == "F"]
    fem = [float(str(i.seconds) + "." + str(i.microseconds)) for i in fem]
    mas = [i[2] for i in results if i[0] == "M"]
    mas = [float(str(i.seconds) + "." + str(i.microseconds)) for i in mas]
    agegroups=[i[1] for i in results if i[0]=="F"]

    plt.plot(agegroups, fem, label="fem")
    plt.plot(agegroups, mas, label="mas")
    plt.title("AVG swimtime per agegroup")
    plt.ylabel('AVG swimtime')
    plt.xlabel('Agegroups')
    plt.legend()
    plt.show()
query5()

def query6():
    sql1 = "select * \
        from (\
                    select meet.name as meet_name, \
            club.name as club_name,\
            athlete.firstname as afn,\
            athlete.lastname as aln,\
            athlete.gender as a_gender,\
            sum(fact.points) as total_points,\
            row_number() over (partition by meet.name order by sum(fact.points) desc) as rn \
        from fact, meet, club, athlete\
        where fact.meetid = meet.meetid and fact.code = club.code and fact.license = athlete.license and athlete.gender ='F'\
        group by club_name, meet.name, afn, aln, a_gender\
        order by meet_name, total_points desc, rn\
        ) as r\
        where r.rn <= 3\
        order by r.meet_name"
    cursor_psql.execute(sql1)
    results1 = cursor_psql.fetchall()
    events = []
    full_names = []
    points = []
    for item in results1:
        if item[0] not in events:
            events.append(item[0])
        full_names.append(item[2] + ' ' + item[3])
        points.append(item[5])
    for i in range(len(events)):
        fig = plt.figure(figsize=(6, 5))
        plt.bar(full_names[i * 3:i * 3 + 3], points[i * 3:i * 3 + 3], color='salmon',
                width=0.6)

        plt.xlabel("Top 3 athletes")
        plt.ylabel("Scores")
        plt.title(events[i])
        plt.show()
query6()

def query7():
    sql1 = "select * \
        from (\
                    select meet.name as meet_name, \
            club.name as club_name,\
            athlete.firstname as afn,\
            athlete.lastname as aln,\
            athlete.gender as a_gender,\
            sum(fact.points) as total_points,\
            row_number() over (partition by meet.name order by sum(fact.points) desc) as rn \
        from fact, meet, club, athlete\
        where fact.meetid = meet.meetid and fact.code = club.code and fact.license = athlete.license and athlete.gender ='M'\
        group by club_name, meet.name, afn, aln, a_gender\
        order by meet_name, total_points desc, rn\
        ) as r\
        where r.rn <= 3\
        order by r.meet_name"
    cursor_psql.execute(sql1)
    results1 = cursor_psql.fetchall()
    events = []
    full_names = []
    points = []
    for item in results1:
        if item[0] not in events:
            events.append(item[0])
        full_names.append(item[2] + ' ' + item[3])
        points.append(item[5])
    for i in range(len(events)):
        fig = plt.figure(figsize=(6, 5))
        plt.bar(full_names[i * 3:i * 3 + 3], points[i * 3:i * 3 + 3], color='turquoise',
                width=0.6)

        plt.xlabel("Top 3 athletes")
        plt.ylabel("Scores")
        plt.title(events[i])
        plt.show()
query7()

def query8():
    sql = "select club.\"name\" as clubname, \
                fact.place,\
                count(club.\"name\") as numofmedals \
            from fact, club\
            where fact.place = 1 and fact.code  = club.code \
            group by clubname, fact.place\
            order by numofmedals desc\
            limit 10"
    cursor_psql.execute(sql)
    results = cursor_psql.fetchall()
    for res in results:
        print(res)

    clubs = [i[0] for i in results]
    numMedals = [i[2] for i in results]

    plt.figure(figsize=(9, 5))
    plt.barh(clubs, numMedals)
    plt.title('Top 10 Clubs with first place')
    plt.xlabel('Number of First Place')
    plt.subplots_adjust(left=0.4, right=0.8, top=0.9, bottom=0.1)
    plt.show()
query8()

def query9():
    sql = "select club.\"name\" as clubname, \
                fact.place,\
                count(club.\"name\") as numofmedals \
            from fact, club\
            where fact.place = 1 and fact.code  = club.code \
            group by clubname, fact.place\
            order by numofmedals desc\
            limit 10"
    cursor_psql.execute(sql)
    results = cursor_psql.fetchall()
    for res in results:
        print(res)

    clubs = [i[0] for i in results]
    numMedals = [i[2] for i in results]

    plt.figure(figsize=(9, 5))
    plt.barh(clubs, numMedals)
    plt.title('Top 10 Clubs with first place')
    plt.xlabel('Number of First Place')
    plt.subplots_adjust(left=0.4, right=0.8, top=0.9, bottom=0.1)
    plt.show()
query9()

def query10():
    sql = "select athlete.gender, club.name, swimstyle.stroke, swimstyle.distance, \
	        meet.name, sum(points) as total_points, min(swimtime) as record \
        from fact, athlete, club, swimstyle, meet \
        where fact.license = athlete.license and fact.code = club.code \
		    and fact.swimstylecode = swimstyle.swimstylecode \
		    and fact.meetid = meet.meetid \
        group by rollup (meet.name,swimstyle.stroke,swimstyle.distance, \
			club.name,athlete.gender)"
    cursor_psql.execute(sql)
    results = cursor_psql.fetchall()
    for res in results:
        print(res)
query10()

def query11():
    sql = "select athlete.gender, club.name, swimstyle.stroke, swimstyle.distance,  \
	        meet.name, max(points) as total_points, min(swimtime) as record \
        from fact, athlete, club, swimstyle, meet \
        where fact.license = athlete.license and fact.code = club.code \
		    and fact.swimstylecode = swimstyle.swimstylecode \
		    and fact.meetid = meet.meetid \
        group by cube (meet.name,swimstyle.stroke,swimstyle.distance, \
			club.name,athlete.gender)"
    cursor_psql.execute(sql)
    results = cursor_psql.fetchall()
    for res in results:
        print(res)
query11()


