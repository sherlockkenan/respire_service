
# A dictionary of movie critics and their ratings of a small
# set of movies

critics = {}
length = 5

from math import sqrt
import csv
import MySQLdb


# Returns a distance-based similarity score for person1 and person2
def sim_distance(prefs, person1, person2):
    # Get the list of shared_items
    si = {}
    for item in prefs[person1]:
        if item in prefs[person2]: si[item] = 1

    # if they have no ratings in common, return 0
    if len(si) == 0: return 0

    # Add up the squares of all the differences
    sum_of_squares = sum([pow(prefs[person1][item] - prefs[person2][item], 2)
                          for item in prefs[person1] if item in prefs[person2]])

    return 1 / (1 + sum_of_squares)


# Returns the Pearson correlation coefficient for p1 and p2
def sim_pearson(prefs, p1, p2):
    # Get the list of mutually rated items
    si = {}
    for item in prefs[p1]:
        if item in prefs[p2]: si[item] = 1

    # if they are no ratings in common, return 0
    if len(si) == 0: return 0

    # Sum calculations
    n = len(si)

    # Sums of all the preferences
    sum1 = sum([prefs[p1][it] for it in si])
    sum2 = sum([prefs[p2][it] for it in si])

    # Sums of the squares
    sum1Sq = sum([pow(prefs[p1][it], 2) for it in si])
    sum2Sq = sum([pow(prefs[p2][it], 2) for it in si])

    # Sum of the products
    pSum = sum([prefs[p1][it] * prefs[p2][it] for it in si])

    # Calculate r (Pearson score)
    num = pSum - (sum1 * sum2 / n)
    den = sqrt((sum1Sq - pow(sum1, 2) / n) * (sum2Sq - pow(sum2, 2) / n))
    if den == 0: return 0

    r = num / den

    return r


# Returns the best matches for person from the prefs dictionary.
# Number of results and similarity function are optional params.
def topMatches(prefs, person, n=5, similarity=sim_pearson):
    scores = [(similarity(prefs, person, other), other)
              for other in prefs if other != person]
    scores.sort()
    scores.reverse()
    return scores[0:n]


# Gets recommendations for a person by using a weighted average
# of every other user's rankings
def getRecommendations(prefs, person, similarity=sim_pearson):
    totals = {}
    simSums = {}
    for other in prefs:
        # don't compare me to myself
        if other == person: continue
        sim = similarity(prefs, person, other)

        # ignore scores of zero or lower
        if sim <= 0: continue
        for item in prefs[other]:

            # only score movies I haven't seen yet
            if item not in prefs[person] or prefs[person][item] == 0:
                # Similarity * Score
                totals.setdefault(item, 0)
                totals[item] += prefs[other][item] * sim
                # Sum of similarities
                simSums.setdefault(item, 0)
                simSums[item] += sim

    # Create the normalized list
    rankings = [(total / simSums[item], item) for item, total in totals.items()]

    # Return the sorted list
    rankings.sort()
    rankings.reverse()
    return rankings


def transformPrefs(prefs):
    result = {}
    for person in prefs:
        for item in prefs[person]:
            result.setdefault(item, {})

            # Flip item and person
            result[item][person] = prefs[person][item]
    return result


def calculateSimilarItems(prefs, n=10):
    # Create a dictionary of items showing which other items they
    # are most similar to.
    result = {}
    # Invert the preference matrix to be item-centric
    itemPrefs = transformPrefs(prefs)
    c = 0
    for item in itemPrefs:
        # Status updates for large datasets
        c += 1
        if c % 100 == 0: print "%d / %d" % (c, len(itemPrefs))
        # Find the most similar items to this one
        scores = topMatches(itemPrefs, item, n=n, similarity=sim_distance)
        result[item] = scores
    return result


def getRecommendedItems(prefs, itemMatch, user):
    userRatings = prefs[user]
    scores = {}
    totalSim = {}
    # Loop over items rated by this user
    for (item, rating) in userRatings.items():

        # Loop over items similar to this one
        for (similarity, item2) in itemMatch[item]:

            # Ignore if this user has already rated this item
            if item2 in userRatings: continue
            # Weighted sum of rating times similarity
            scores.setdefault(item2, 0)
            scores[item2] += similarity * rating
            # Sum of all the similarities
            totalSim.setdefault(item2, 0)
            totalSim[item2] += similarity

    # Divide each total score by total weighting to get an average
    rankings = [(score / totalSim[item], item) for item, score in scores.items()]

    # Return the rankings from highest to lowest
    rankings.sort()
    rankings.reverse()
    return rankings


def loadMovieLens(path='/data/movielens'):
    # Get movie titles
    movies = {}
    for line in open(path + '/u.item'):
        (id, title) = line.split('|')[0:2]
        movies[id] = title

    # Load data
    prefs = {}
    for line in open(path + '/u.data'):
        (user, movieid, rating, ts) = line.split('\t')
        prefs.setdefault(user, {})
        prefs[user][movies[movieid]] = float(rating)
    return prefs


def loaddata():
    db = MySQLdb.connect("localhost", "root", "root", "respire")
    cursor = db.cursor()
    sql = "SELECT * from click"
    #0:clickid 1:itemid 2:length 3:userid
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        for line in results:
            if critics.has_key(line[3]):
                oldscore = critics[line[3]]
                oldscore[line[1]] = float(line[2])
            else:
                score = {}
                score[line[1]] = float(line[2])
                critics[line[3]] = score
        print critics
    except:
        print "Error: unable to fecth data"

#first clean the recommand table
def writeresult(rvalue):
    db = MySQLdb.connect("localhost", "root", "root", "respire")
    cursor = db.cursor()
    sql = "DELETE FROM recommand"
    try:
        cursor.execute(sql)
        db.commit()
    except:
        db.rollback()
    for key in rvalue:
        tmp = rvalue[key]
        sql = 'INSERT INTO recommand(userid,itemid1,itemid2,itemid3,itemid4,itemid5)VALUES (%s,%s,%s,%s,%s,%s)'
        try:
            value = [key, tmp[0], tmp[1], tmp[2], tmp[3], tmp[4]]
            cursor.execute(sql, value)
            db.commit()
        except :

            db.rollback()
    db.close()


def process():
    db = MySQLdb.connect("localhost", "root", "root", "respire")
    cursor = db.cursor()
    sql = "SELECT userid from users"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        rvalue = {}
        for line in results:
            print line
            tmp = getRecommendations(critics, line[0])
            itemid = []
            i = 0
            for t in tmp:
                itemid.append(t[1])
                i = i + 1
                if i == length:
                    break
            while len(itemid) < length:
                itemid.append('0')
            rvalue[line[0]] = itemid
    except:
        print "Error: unable to fecth data"
    return rvalue


def main():
    loaddata()
    rvalue = process()
    writeresult(rvalue)


if __name__ == "__main__":
    main()
