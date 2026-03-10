DELETE FROM ui.emailtemplate
WHERE id IN (1, 2, 3, 4, 5)
  AND associateid = 'internal'
  AND itenantid = 0;

DELETE FROM ui.templateresponse
WHERE templateid = 44
  AND templatename = 'HealthClaim';
