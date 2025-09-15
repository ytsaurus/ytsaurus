{{- define "odin.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}


{{- define "odin.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}


{{- define "odin.labels" -}}
app.kubernetes.io/name: {{ include "odin.name" . }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}


{{- define "odin.selectorLabels" -}}
app.kubernetes.io/name: {{ include "odin.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "odin.enableIPv4" -}}
{{- if .Values.config.odin.useIPv4 -}}
{{- .Values.config.odin.useIPv4 -}}
{{- else -}}
{{- not .Values.config.odin.useIPv6 -}}
{{- end -}}
{{- end -}}

{{- define "odin.enableIPv6" -}}
{{- if .Values.config.odin.useIPv6 -}}
{{- .Values.config.odin.useIPv6 -}}
{{- else -}}
{{- false -}}
{{- end -}}
{{- end -}}
