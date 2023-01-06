import csv

# Open the input CSV file
csvfile = open("C:\\Retail_log_project\\input_file\\Retail.csv", 'r')

# Create a CSV reader
reader = csv.reader(csvfile)

# Open the output CSV file for writing if dosn't exist will create new one with same name
outfile = open("C:\\Retail_log_project\\input_file\\new_Retail11.csv", 'w', newline='')

# Create a CSV writer
writer = csv.writer(outfile)

# Get the header row
header = next(reader)

# Write the header row to the output file
writer.writerow(header)

# Iterate over the remaining rows in the input file
for i in range(3):
    s=-1
    csvfile.seek(0)  # go back to the beginning of the file
    next(reader)  # skip the header row
    for row in reader:
        #Write the row to the output file
        #Make the desired changes to the row values
        o = int(row[0].split("SO")[1])
        A = o + 1000 * (s + 1)
        # changing the ORDER NUMBER
        row[0] = row[0].replace("SO" + str(o), "SO" + str(A), 1)

        # changing the ORDER DATE
        od = int(row[6].split("/")[1])
        B = s + 1
        row[6] = row[6].replace(str(od), str(B), 1)

        # changing the DUE DATE
        dd = int(row[7].split("/")[1])
        C = s + 1
        row[7] = row[7].replace(str(dd), str(C), 1)

        # changing the SHIP DATE
        sd = int(row[8].split("/")[1])
        D = s + 1
        row[8] = row[8].replace(str(sd), str(D), 1)

        # changing the UNITPRICE
        row[12] = float(row[12].strip(" ")) + 500 * (s + 1)
        writer.writerow(row)

        s = s + 1

# Close the files
csvfile.close()
outfile.close()



