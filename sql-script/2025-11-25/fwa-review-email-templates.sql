-- FWA Request Info Email Template
INSERT INTO ui.emailtemplate(id, body, subject, associateid, itenantid)
VALUES (16,
'<p>Hi <span th:text="${name}"> Bob </span>,</p>
<br>
<p>After reviewing your claim for fraud/waste/abuse indicators, we need additional information.</p>
<p>Policy Id - <span th:text="${policyId}">Transaction ID</span></p>
<p>Please provide:</p>
<ul>
<li>Additional documentation supporting the claim</li>
<li>Clarification on flagged items</li>
<li>Any relevant medical records</li>
</ul>
<p>Thanks & Regards,</p>
<p>Claims Review Team</p>',
'Request for Additional FWA Information', 'internal', 1);

-- FWA Reminder Email Template
INSERT INTO ui.emailtemplate(id, body, subject, associateid, itenantid)
VALUES (17,
'<p>Hi <span th:text="${name}"> Bob </span>,</p>
<br>
<p>This is a reminder regarding your pending claim review.</p>
<p>Policy Id - <span th:text="${policyId}">Transaction ID</span></p>
<p>We still need the additional information requested earlier.</p>
<p>Thanks & Regards,</p>
<p>Claims Review Team</p>',
'Reminder: Additional FWA Information Required', 'internal', 1);

-- FWA Missing Info Rejection Email Template
INSERT INTO ui.emailtemplate(id, body, subject, associateid, itenantid)
VALUES (18,
'<p>Hi <span th:text="${name}"> Bob </span>,</p>
<br>
<p>We regret to inform you that your claim has been rejected due to incomplete information.</p>
<p>Policy Id - <span th:text="${policyId}">Transaction ID</span></p>
<p>We did not receive the requested documentation within the required timeframe.</p>
<p>Thanks & Regards,</p>
<p>Claims Review Team</p>',
'Claim Rejected - Missing Information', 'internal', 1);

-- FWA Rejection Email Template
INSERT INTO ui.emailtemplate(id, body, subject, associateid, itenantid)
VALUES (19,
'<p>Hi <span th:text="${name}"> Bob </span>,</p>
<br>
<p>After careful review, your claim has been rejected.</p>
<p>Policy Id - <span th:text="${policyId}">Transaction ID</span></p>
<p>The claim raised fraud/waste/abuse concerns that could not be resolved.</p>
<p>Please contact your TPA agent for more information.</p>
<p>Thanks & Regards,</p>
<p>Claims Review Team</p>',
'Insurance Claim Rejected - FWA Review', 'internal', 1);