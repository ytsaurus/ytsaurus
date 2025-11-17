{{/*
Expand the name of the chart.
*/}}
{{- define ".name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define ".fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name as used by the chart label.
*/}}
{{- define ".chart" -}}
{{- printf "%s" .Chart.Name | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define ".labels" -}}
helm.sh/chart: {{ include ".chart" . }}
{{ include ".selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define ".selectorLabels" -}}
app.kubernetes.io/name: {{ include ".name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define ".serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include ".fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define ".cronJobSlug" -}}
{{ .name | replace "_" "-" }}
{{- end }}

{{/*
Container body template
*/}}
{{- define ".containerBody" -}}
securityContext:
  {{- toYaml .Values.securityContext | nindent 2 }}
image: {{ .Values.image.repository }}:{{ .Values.image.tag | default (printf "v%s" .Chart.AppVersion) }}
imagePullPolicy: {{ .Values.image.pullPolicy }}
env:
  - name: YT_PROXY
    value: {{ .Values.yt.proxy | quote }}
  - name: YT_TOKEN
    valueFrom:
      secretKeyRef:
        {{- if not .Values.unmanagedSecret.enabled }}
        key: YT_TOKEN
        name: {{ include ".fullname" . }}
        {{- else }}
        key: {{ .Values.unmanagedSecret.secretKeyRef.key }}
        name: {{ .Values.unmanagedSecret.secretKeyRef.name }}
        {{- end }}
resources:
  {{- toYaml .Values.resources | nindent 2 }}
{{- with .Values.volumeMounts }}
volumeMounts:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}
