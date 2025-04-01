import json

debug = True

def cleanStr4SQL(s):
    return s.replace("'","`").replace("\n"," ")

def int2BoolStr (value):
    if value == 0:
        return 'false'
    else:
        return 'true'

def get_attributes(attributes):
    L = []
    for (attribute, value) in list(attributes.items()):
        if isinstance(value, dict):
            L += get_attributes(value)
        else:
            L.append((attribute, value))
    return L

def process_business_data():
    #reading the JSON file
    with open('.//yelp_business.JSON','r') as f:
        outfile =  open('.//yelp_business_output.txt', 'w')
        line = f.readline()
        count_line = 0


        while line:
            data = json.loads(line)
            if debug:
                outfile.write("{} - business info : '{}' ; '{}' ; '{}' ; '{}' ; '{}' ; '{}' ; {} ; {} ; {} ; {}\n".format(
                              str(count_line), 
                              cleanStr4SQL(data['business_id']),
                              cleanStr4SQL(data["name"]),
                              cleanStr4SQL(data["address"]),
                              cleanStr4SQL(data["state"]),
                              cleanStr4SQL(data["city"]),
                              cleanStr4SQL(data["postal_code"]),
                              str(data["latitude"]),
                              str(data["longitude"]),
                              str(data["stars"]),
                              str(data["is_open"])) )

            #process business categories
            bid = cleanStr4SQL(data['business_id'])
            categories = data["categories"].split(', ')
            if debug:
                outfile.write("      categories: {}\n".format(str(categories)))

            #process business attributes
            attributes =  get_attributes(data['attributes'])
            if debug:
                outfile.write("      attributes {} : \n".format(str(attributes)))

            if debug:
                outfile.write("      hours: {} \n".format(str([(day,value.split('-')) for (day,value) in data["hours"].items()])))

            line = f.readline()
            count_line +=1



    print(count_line)
    outfile.close()
    f.close()



def split_checkins(datestr):
    return datestr.split(',')

def process_checkin_data():
    #reading the JSON file
    with open('.//yelp_checkin.JSON','r') as f:
        outfile =  open('.//yelp_checkin_output.txt', 'w')

        line = f.readline()
        count_line = 1
        failed_inserts = 0

        while line:
            data = json.loads(line)
            bid = cleanStr4SQL(data['business_id'])
            checkin_list = split_checkins(data["date"])
            outfile.write("{} - '{}':\n          ".format(str(count_line), cleanStr4SQL(data['business_id'])))
            for timestamp in checkin_list:
                year = timestamp[:4]
                month = timestamp[5:7]
                day = timestamp[8:10]
                time = timestamp[11:]
                outfile.write( "( '{}' , '{}' , '{}' , '{}' )\t".format(
                                str(year),
                                str(month),
                                str(day),
                                str(time)) )
            
            outfile.write("\n")
            line = f.readline()
            count_line +=1
    print(count_line)
    outfile.close()
    f.close()

def process_user_data():
    #reading the JSON file
    with open('.//yelp_user.JSON','r') as f:
        outfile =  open('.//yelp_user_output.txt', 'w')
        line = f.readline()
        count_line = 0

        while line:
            data = json.loads(line)
            outfile.write("{} - user info: '{}' ; '{}' ; '{}' ; {} ; {} ; {} ; ({} , {}, {}) \n".format(
                          str(count_line) , 
                          cleanStr4SQL(data['user_id']) , 
                          cleanStr4SQL(data["name"]) , 
                          cleanStr4SQL(data["yelping_since"]) ,
                          str(data["tipcount"]), 
                          str(data["fans"]), 
                          str(data["average_stars"]),
                          str(data["funny"]), str(data["useful"]), str(data["cool"]))
            )
            outfile.write("    friends: {}\n".format(str(data["friends"])))
            line = f.readline()
            count_line +=1

    print(count_line)
    outfile.close()
    f.close()


def process_tip_data():
    #reading the JSON file
    with open('.//yelp_tip.JSON','r') as f:
        outfile =  open('.//yelp_tip_output.txt', 'w')
        line = f.readline()
        count_line = 1
        failed_inserts = 0

        while line:
            data = json.loads(line)
            outfile.write("{} - '{}' ; '{}' ; {} ; '{}' ; '{}'\n".format(
                            str(count_line) , 
                            cleanStr4SQL(data["business_id"]) , 
                            cleanStr4SQL(data["date"]) ,
                            str(data["likes"]) , 
                            cleanStr4SQL(data["text"]) , 
                            cleanStr4SQL(data['user_id'])))
            count_line +=1
            line = f.readline()
    print(count_line)
    outfile.close()
    f.close()



process_business_data()
process_checkin_data()
process_user_data()
process_tip_data()


