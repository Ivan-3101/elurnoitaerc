INSERT INTO ui.emailtemplate(
	id, body, subject, associateid,  itenantid)
	VALUES (1, '<p>Hii <span th:text="${name}"> Bob </span>,</p>
<br>
<p>We have received claim intimation,below are its details</p>
<p>Policy Id - <span th:text="${policyId}">Tranaction ID</span></p>
<p>Please add additional details like pathology report / payment receipt / OPD reports</span></p>
<p>Thanks Regards,</p>
<p>Claim Team</p>', 'Request Info for Insurance Claim', 'internal',0);


INSERT INTO ui.emailtemplate(
	id, body, subject, associateid,  itenantid)
	VALUES (2, '<p>Hii <span th:text="${name}"> Bob </span>,</p>
<br>
<p>We have received claim intimation,below are its details</p>
<p>Policy Id - <span th:text="${policyId}">Tranaction ID</span></p>
<p>Please add additional details like pathology report / payment receipt / OPD reports</span></p>
<p>Thanks Regards,</p>
<p>Claim Team</p>', 'Reminder: Request Info for Claim Intimation', 'internal',0);

INSERT INTO ui.emailtemplate(
	id, body, subject, associateid,  itenantid)
	VALUES (3, '<p>Hii <span th:text="${name}"> Bob </span>,</p>
<br>
<p>We have received claim intimation,below are its details</p>
<p>Policy Id - <span th:text="${policyId}">Tranaction ID</span></p>
<p>WE hereby sorry to inform you thath your claim has been rejected due missing information.</span></p>
<p>Thanks Regards,</p>
<p>Claim Team</p>', 'Missing Info Claim Rejection', 'internal',0);

INSERT INTO ui.emailtemplate(
	id, body, subject, associateid,  itenantid)
	VALUES (4, '<p>Hii <span th:text="${name}"> Bob </span>,</p>
<br>
<p>We have received claim intimation,below are its details</p>
<p>Policy Id - <span th:text="${policyId}">Tranaction ID</span></p>
<p>After careful observation we have found claim is not valid.Kindly please reach out your TPA agent for more info. </span></p>
<p>Thanks Regards,</p>
<p>Claim Team</p>', 'Insurance Claim Rejected', 'internal',0);

INSERT INTO ui.emailtemplate(
	id, body, subject, associateid,  itenantid)
	VALUES (5, '<p>Hii <span th:text="${name}"> Bob </span>,</p>
<br>
<p>We have received claim intimation,below are its details</p>
<p>Policy Id - <span th:text="${policyId}">Tranaction ID</span></p>
<p>We hereby inform your claim has been approved and amount will credited to your bank accound within 5-7 working days.</span></p>
<p>Thanks Regards,</p>
<p>Claim Team</p>', 'Insurance Claim Approved', 'internal',0);

INSERT INTO ui.templateresponse(templateid, activeflag, jsonresponse, responses, templatename) VALUES (44, 'Y', '', 'CallBack','HealthClaim');