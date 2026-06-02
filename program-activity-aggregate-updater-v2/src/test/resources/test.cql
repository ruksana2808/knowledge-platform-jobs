CREATE KEYSPACE sunbird_courses with replication = {'class':'SimpleStrategy','replication_factor':1};

CREATE TABLE sunbird_courses.user_content_consumption (
    userid text,
    courseid text,
    batchid text,
    contentid text,
    completedcount int,
    datetime timestamp,
    lastaccesstime text,
    lastcompletedtime text,
    lastupdatedtime text,
    progress int,
    status int,
    viewcount int,
    PRIMARY KEY (userid, courseid, batchid, contentid)
) WITH CLUSTERING ORDER BY (courseid ASC, batchid ASC, contentid ASC);

CREATE TABLE IF NOT EXISTS sunbird_courses.user_activity_agg (
    activity_id text,
    user_id text,
    activity_type text,
    context_id text,
    agg Map<text, int>,
    aggregates Map<text, double>,
    agg_last_updated Map<text, timestamp>,
    PRIMARY KEY ((activity_type, activity_id), context_id, user_id)
);

CREATE TABLE sunbird_courses.user_enrolments (
    userid text,
    courseid text,
    batchid text,
    active boolean,
    addedby text,
    certificates list<frozen<map<text, text>>>,
    completedon timestamp,
    completionpercentage int,
    contentstatus map<text, int>,
    datetime timestamp,
    enrolleddate text,
    issued_certificates list<frozen<map<text, text>>>,
    lastreadcontentid text,
    lastreadcontentstatus int,
    progress int,
    status int,
    PRIMARY KEY (userid, courseid, batchid)
) WITH CLUSTERING ORDER BY (courseid ASC, batchid ASC);

CREATE INDEX inx_ues_status ON sunbird_courses.user_enrolments (status);

-- Test data: user001 enrolled in course001 / Batch1 with status=1 (in-progress)
INSERT INTO sunbird_courses.user_enrolments(userid, courseid, batchid, status, active, contentstatus)
  VALUES ('user001', 'course001', 'Batch1', 1, true, {'do_R1':2,'do_R2':1});

-- Test data: user002 enrolled and already completed
INSERT INTO sunbird_courses.user_enrolments(userid, courseid, batchid, status, active, contentstatus)
  VALUES ('user002', 'course001', 'Batch1', 2, true, {'do_R1':2,'do_R2':2,'do_R3':2});

