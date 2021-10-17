import mrs


class Normalisation(mrs.MapReduce):

    def input_data(self, job):
        return job.file_data([self.args[0]])

    def map(self, key, value):
        row = value.split(",")

        age = row[1]
        height_cm = row[2]
        weight_kg = row[3]
        sex = row[4]
        # print "age", age, "weight", weight_kg, "height", height_cm
        if age.isdigit() and age > 18:
            imc = self.imc(weight_kg, height_cm)
            yield sex, imc

    def reduce(self, key, values):
        value_list = list(values)
        yield sum(value_list)/len(value_list)
        yield min(value_list)
        yield max(value_list)

    def imc(self, weight_kg, height_cm):
        weight = float(weight_kg)
        height = float(height_cm)/100
        height_carre = height * height
        imc = weight/height_carre
        return imc


if __name__ == '__main__':
    mrs.main(Normalisation)

