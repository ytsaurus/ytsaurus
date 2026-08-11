# Maintaining the field order in a typed stream

{% note warning %}

You must ensure that the order of fields in the typed data model matches the order of columns in the stream definition in the static spec. If you break the field order, it can lead to hard-to-diagnose errors.

{% endnote %}