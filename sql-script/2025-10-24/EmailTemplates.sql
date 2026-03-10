-- Insert email templates for tenant ID 1
INSERT INTO ui.emailtemplate(id, body, subject, associateid, itenantid)
VALUES
(11, '<p>Hi <span th:text="${name}"> Bob </span>,</p>
<br>
<p>We have received claim intimation, below are its details</p>
<p>Policy Id - <span th:text="${policyId}">Transaction ID</span></p>
<p>Please add additional details like pathology report / payment receipt / OPD reports</p>
<p>Thanks & Regards,</p>
<p>Claim Team</p>',
'Request Info : Insurance Claim', 'internal', 1),

(12, '<p>Hi <span th:text="${name}"> Bob </span>,</p>
<br>
<p>We have received claim intimation, below are its details</p>
<p>Policy Id - <span th:text="${policyId}">Transaction ID</span></p>
<p>Please add additional details like pathology report / payment receipt / OPD reports</p>
<p>Thanks & Regards,</p>
<p>Claim Team</p>',
'Reminder: Request Info for Claim Intimation', 'internal', 1),

(13, '<p>Hi <span th:text="${name}"> Bob </span>,</p>
<br>
<p>We have received claim intimation, below are its details</p>
<p>Policy Id - <span th:text="${policyId}">Transaction ID</span></p>
<p>We hereby sorry to inform you that your claim has been rejected due to missing information.</p>
<p>Thanks & Regards,</p>
<p>Claim Team</p>',
'Missing Info Claim Rejection', 'internal', 1),

(14, '<p>Hi <span th:text="${name}"> Bob </span>,</p>
<br>
<p>We have received claim intimation, below are its details</p>
<p>Policy Id - <span th:text="${policyId}">Transaction ID</span></p>
<p>After careful observation we have found the claim is not valid. Kindly please reach out to your TPA agent for more info.</p>
<p>Thanks & Regards,</p>
<p>Claim Team</p>',
'Insurance Claim Rejected', 'internal', 1),

(15, '<p>Hi <span th:text="${name}"> Bob </span>,</p>
<br>
<p>We have received claim intimation, below are its details</p>
<p>Policy Id - <span th:text="${policyId}">Transaction ID</span></p>
<p>We hereby inform you that your claim has been approved and the amount will be credited to your bank account within 5-7 working days.</p>
<p>Thanks & Regards,</p>
<p>Claim Team</p>',
'Insurance Claim Approved', 'internal', 1);

-- Update template response for tenant 1
INSERT INTO ui.templateresponse(templateid, activeflag, jsonresponse, responses, templatename)
VALUES (45, 'Y', '', 'CallBack', 'HealthClaim');