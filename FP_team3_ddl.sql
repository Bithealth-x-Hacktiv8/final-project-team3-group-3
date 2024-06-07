CREATE TABLE "Admission" (
  "id" int PRIMARY KEY,
  "date_in" date,
  "date_out" date,
  "id_branch" varchar,
  "hospital_care" varchar,
  "id_patient" int,
  "id_room" int,
  "id_doctor" int,
  "surgery_id" int,
  "lab_id" int,
  "id_drug" int,
  "drug_quantity" int,
  "admin_price" currency,
  "cogs" currency,
  "id_payment" int,
  "id_review" int
);

CREATE TABLE "Doctor" (
  "id" int PRIMARY KEY,
  "doctor_type" varchar
);

CREATE TABLE "Payment" (
  "id" int PRIMARY KEY,
  "payment_type" varchar
);

CREATE TABLE "Drug_Category" (
  "id" int PRIMARY KEY,
  "category" varchar
);

CREATE TABLE "Review" (
  "id" int PRIMARY KEY,
  "review" varchar
);

CREATE TABLE "Surgery" (
  "id" int PRIMARY KEY,
  "surgery_type" varchar
);

CREATE TABLE "Lab" (
  "id" int PRIMARY KEY,
  "lab_name" varchar
);

CREATE TABLE "Room_Type" (
  "id" int PRIMARY KEY,
  "room_type" varchar,
  "food_price" currency
);

CREATE TABLE "Drugs" (
  "id" int PRIMARY KEY,
  "drug_name" varchar,
  "id_category" varchar
);

CREATE TABLE "Patient" (
  "id" int PRIMARY KEY,
  "patient_name" varchar,
  "gender" varchar,
  "age" int
);

CREATE TABLE "Stock_Obat" (
  "id" int PRIMARY KEY,
  "date" date,
  "id_drug" int,
  "qty" int,
  "id_branch" int
);

CREATE TABLE "Branch" (
  "id" int PRIMARY KEY,
  "branch_name" varchar
);

ALTER TABLE "Admission" ADD FOREIGN KEY ("id_patient") REFERENCES "Patient" ("id");

ALTER TABLE "Admission" ADD FOREIGN KEY ("id_doctor") REFERENCES "Doctor" ("id");

ALTER TABLE "Surgery" ADD FOREIGN KEY ("id") REFERENCES "Admission" ("surgery_id");

ALTER TABLE "Lab" ADD FOREIGN KEY ("id") REFERENCES "Admission" ("lab_id");

ALTER TABLE "Drugs" ADD FOREIGN KEY ("id") REFERENCES "Admission" ("id_drug");

ALTER TABLE "Room_Type" ADD FOREIGN KEY ("id") REFERENCES "Admission" ("id_room");

ALTER TABLE "Admission" ADD FOREIGN KEY ("id_branch") REFERENCES "Branch" ("id");

ALTER TABLE "Admission" ADD FOREIGN KEY ("id_payment") REFERENCES "Payment" ("id");

ALTER TABLE "Admission" ADD FOREIGN KEY ("id_review") REFERENCES "Review" ("id");

ALTER TABLE "Drugs" ADD FOREIGN KEY ("id_category") REFERENCES "Drug_Category" ("id");

ALTER TABLE "Stock_Obat" ADD FOREIGN KEY ("id_drug") REFERENCES "Drugs" ("id");

ALTER TABLE "Stock_Obat" ADD FOREIGN KEY ("id_branch") REFERENCES "Branch" ("id");
