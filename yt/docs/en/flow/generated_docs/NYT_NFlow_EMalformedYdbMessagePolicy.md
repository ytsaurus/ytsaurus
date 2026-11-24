## NYT::NFlow::EMalformedYdbMessagePolicy {#NYT_NFlow_EMalformedYdbMessagePolicy}
<!-- This page is generated. -->

#|
|| Possible values | **Description** ||
|| `keep` | Emit one Flow message with `data = null` and the original YDB message represented as structured YSON in `unparsed`; its offset can then be committed. ||
|| `drop` | Emit no Flow message for the malformed YDB message and continue reading, allowing its offset to be committed. ||
|| `fail` | Fail processing. The message is not considered processed and is read again after the job recovers. ||
|#

