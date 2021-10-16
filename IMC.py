import os
import re

import mrs
import csv


class Normalisation(mrs.MapReduce):

    def input_data(self, job):
        return job.file_data([self.args[0]])

    def map(self, key, value):
        row = value.split(",")

        age = row[1]
        height_cm = row[2]
        weight_kg = row[3]
        sex = row[4]
        print "age", age, "weight", weight_kg, "height", height_cm
        if age.isdigit() and age > 18:
            imc = self.imc(weight_kg, height_cm)
            yield sex, imc

    def reduce(self, key, values):
        value_list = list(values)
        yield sum(value_list)/len(value_list), min(value_list), max(value_list)

    def imc(self, weight_kg, height_cm):
        imc = int(weight_kg)/(int(height_cm)/100)**2
        return imc


if __name__ == '__main__':
    mrs.main(Normalisation)

