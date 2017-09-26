#!/usr/bin/python
import sys
import os
import io
import psycopg2 as pg
import smtplib

fromaddr="cmshared@n5eil10.ecs.nsidc.org"
toaddr="mike.laxer@nsidc.org"
#toaddr="ops@nsidc.org"

class daQueue():

    def psql(self):

        self.results = []

        try:
            self.con = None
            self.start = 0
            self.con = pg.connect(database='ecs', user='anonymous', host='n4dbl03', port='5430')
            self.query1 = self.con.cursor()
            self.query1.execute("""
                SELECT receivetime as submission_date,
                emailaddress as email,
                priority as priority,
                count(distinct requestid) as num_orders,
                count(jobid) as num_granules
                from aim.amdarequest
                natural join aim.amdajob
                where aim.amdajob.status = 'pending'
                group by 1,2,3
                LIMIT 0"""
            )

            self.colnames = [desc[0] for desc in self.query1.description]

            self.query= self.con.cursor()
            self.query.execute("""
                select date_trunc('day', receivetime)
                as submission_date,
                emailaddress,
                priority,
                count(distinct requestid) as num_orders,
                count(jobid) as num_granules
                from aim.amdarequest
                natural join aim.amdajob
                where aim.amdajob.status = 'pending'
                group by 1,2,3
                order by 3,1,2
                """
            )

            self.datecol = self.colnames[0]
            self.emailcol = self.colnames[1]
            self.prioritycol = self.colnames[2]
            self.nordercol = self.colnames[3]
            self.ngrancol = self.colnames[4]
            self.results.append("%s | %s | %s | %s | %s" % (self.datecol.center(19, ' '), self.emailcol.center(40, ' '), self.prioritycol,self.nordercol.center(10),self.ngrancol.center(10)))
            self.results.append("--------------------+------------------------------------------+----------+------------+-------------")

            self.count = 0

            for table in self.query.fetchall():
                self.datetime = table[0]
                self.email = str(table[1])
                self.priority = str(table[2])
                self.norder = str(table [3])
                self.ngran = str(table[4])
                self.results.append("%s | %s | %s | %s | %s" % (self.datetime, self.email.ljust(40), self.priority.rjust(8), self.norder.rjust(10), self.ngran.rjust(12)))
                self.count+=1

        except pg.DatabaseError, e:
            print 'Error %s' % e
            sys.exit(1)

        finally:
            if self.con:
                self.con.close()

        self.out = '\n'.join(map(str, self.results))


    def mail(self):
        self.subject = 'Data Access Queue Report'
        if self.count > 0:
                self.msg = ("From: %s\nTo: %s\nSubject: %s\nHello Meatbags,\n\n"
                "Here is your daily Data Access Queue Report.  Copy this into the ECS Green Message.\n\n"
                "%s\n\n\n"
                "If there are any questions about this report, contact my organic meatbag maintainer at mike.laxer@nsidc.org") % (
                        fromaddr,
                        toaddr,
                        self.subject,
                        self.out
                )
        else:
                self.msg = ("From: %s\nTo: %s\nSubject: %s\nHello Meatbag,\n\n"
                "The Data Access Queue is empty.  No action is necessary.\n\n\n\n"
                "If there are any questions about this report, contact my organic meatbag maintainer at mike.laxer@nsidc.org") % (
                        fromaddr,
                        toaddr,
                        self.subject
                )

        try:
                smtpObj = smtplib.SMTP('localhost')
                smtpObj.sendmail(fromaddr, toaddr, self.msg)
        except:
                print "Unable to send mail"


def main():
        id=daQueue()
        id.psql()
        id.mail()


if __name__ == "__main__":
        main()
#
