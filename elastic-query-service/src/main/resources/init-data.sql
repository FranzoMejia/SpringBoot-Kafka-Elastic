CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


INSERT INTO public.users(
	id, username, firstname, lastname)
	VALUES ('91f8b108-3ccf-4dd2-b6e9-a046ff08a623', 'app_user', 'Standard', 'User');
INSERT INTO public.users(
	id, username, firstname, lastname)
	VALUES ('5bc70b94-f5b8-41d6-8f4e-cc1f78baf3a1', 'app_admin', 'Admin', 'User');
INSERT INTO public.users(
	id, username, firstname, lastname)
	VALUES ('96da4059-a11e-4139-8694-6f028ec8a4a3', 'app_super_user', 'Super', 'User');


insert into documents(id, document_id)
values ('c1df7d01-4bd7-40b6-86da-7e2ffabf37f7', 1);
insert into documents(id, document_id)
values ('f2b2d644-3a08-4acb-ae07-20569f6f2a01', 2);
insert into documents(id, document_id)
values ('90573d2b-9a5d-409e-bbb6-b94189709a19', 2081680368099620834);

insert into user_permissions(user_permission_id, user_id, document_id, permission_type)
values (uuid_generate_v4(),'c1df7d01-4bd7-40b6-86da-7e2ffabf37f7', 'c1df7d01-4bd7-40b6-86da-7e2ffabf37f7', 'READ');

insert into user_permissions(user_permission_id, user_id, document_id, permission_type)
values (uuid_generate_v4(),'c1df7d01-4bd7-40b6-86da-7e2ffabf37f7', 'c1df7d01-4bd7-40b6-86da-7e2ffabf37f7', 'READ');

insert into user_permissions(user_permission_id, user_id, document_id, permission_type)
values (uuid_generate_v4(),'f2b2d644-3a08-4acb-ae07-20569f6f2a01', 'f2b2d644-3a08-4acb-ae07-20569f6f2a01', 'READ');

insert into user_permissions(user_permission_id, user_id, document_id, permission_type)
values (uuid_generate_v4(), 'f2b2d644-3a08-4acb-ae07-20569f6f2a01', '90573d2b-9a5d-409e-bbb6-b94189709a19', 'READ');

insert into user_permissions(user_permission_id, user_id, document_id, permission_type)
values (uuid_generate_v4(), '90573d2b-9a5d-409e-bbb6-b94189709a19', 'c1df7d01-4bd7-40b6-86da-7e2ffabf37f7', 'READ');


