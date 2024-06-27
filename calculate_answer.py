import numpy as np

# On charge tous les vecteurs de notre base de données de restaurant

myfile = open("bert1.txt", "r")
myline = myfile.readline()

all_vect = []


for i in range(1,3771):
    myline1 = myfile.readline()
    vector = list(myline1[myline1.find("[")+1:len(myline1)-3].split(","))
    vector = [float(item) for item in vector]
    all_vect.append(vector)

myfile.close()

# On charge le vecteur de notre question

myfile1 = open("question_vector.txt", "r")

myline = myfile1.readline()

question_vector = list(myline1[myline1.find("[")+1:len(myline1)-3].split(","))
question_vector = [float(item) for item in question_vector]

# On calcul le vecteur le plus proche 

all_dist = []

for i in range(0,3770):
    a = np.array(all_vect[i])
    b = np.array(question_vector)

    dist = np.linalg.norm(a-b)
    all_dist.append(dist)

our_answer = all_dist.index(min(all_dist))

print("Votre restaurant correspond au restaurant d'index : " + str(our_answer))