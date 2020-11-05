cus_insert_sql =\
"""
INSERT INTO tblsurveycake (name, gender, tel, email, beg_dt, tourist_attraction, sales_score, meal_score, hotel_score, traffic_score, schedule_score, tour_leader_score, tour_guide_score, is_pretrip, is_member, is_comment_letter, suggest, answer_dt, answer_sec, ip, mark, user_record, member_time, member_no, cus_ID, memo, traffic_cause, hotel_cause, guarantee_cause, company_cause, tour_guide_cause, sales_cause, market_activity_cause, outlet_cause, tour_leader_cause, meal_cause, group_no, create_date, modify_date)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, LOCALTIMESTAMP, LOCALTIMESTAMP) ;
"""
