package tech.ytsaurus.core.common;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import NYT.Extension;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.TextFormat;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeStringNode;

public class YTsaurusProtobufFormat {
    private final List<Descriptor> messageDescriptors;

    public YTsaurusProtobufFormat(List<Builder> messageBuilders) {
        this.messageDescriptors = messageBuilders.stream()
                .map(Builder::getDescriptorForType)
                .collect(Collectors.toList());
    }

    public YTreeStringNode spec() {
        FileDescriptorSetBuilder fileDescriptorSetBuilder = new FileDescriptorSetBuilder();
        for (Descriptor descriptor : messageDescriptors) {
            fileDescriptorSetBuilder.addDescriptor(descriptor);
        }

        String shortText = TextFormat.shortDebugString(fileDescriptorSetBuilder.build());

        return YTree.builder()
                .beginAttributes()
                .key("file_descriptor_set_text").value(shortText)
                .key("type_names").value(
                        messageDescriptors.stream()
                                .map(Descriptor::getFullName)
                                .collect(Collectors.toList())
                )
                .endAttributes()
                .value("protobuf")
                .build().stringNode();
    }

    @NonNullApi
    @NonNullFields
    static class FileDescriptorSetBuilder {
        private static final Descriptors.FileDescriptor SYSTEM_EXTENSION_FILE =
                Extension.EWrapperFieldFlag.getDescriptor().getFile();
        private final Set<Descriptor> allDescriptors = new HashSet<>();
        private final Set<EnumDescriptor> allEnumDescriptors = new HashSet<>();

        public void addDescriptor(Descriptor descriptor) {
            if (!allDescriptors.add(descriptor)) {
                return;
            }

            addAllContainingTypes(descriptor.getContainingType());

            for (FieldDescriptor field : descriptor.getFields()) {
                addField(field);
            }
        }

        public DescriptorProtos.FileDescriptorSet build() {
            Set<Descriptors.FileDescriptor> visitedFiles = new HashSet<>();
            List<Descriptors.FileDescriptor> fileTopologicalOrder = new ArrayList<>();
            for (Descriptor descriptor : allDescriptors) {
                traverseDependencies(descriptor.getFile(), visitedFiles, fileTopologicalOrder);
            }
            for (EnumDescriptor enumDescriptor : allEnumDescriptors) {
                traverseDependencies(enumDescriptor.getFile(), visitedFiles, fileTopologicalOrder);
            }

            Set<String> messageNames = allDescriptors.stream()
                    .map(Descriptor::getFullName)
                    .collect(Collectors.toSet());
            Set<String> enumNames = allEnumDescriptors.stream()
                    .map(EnumDescriptor::getFullName)
                    .collect(Collectors.toSet());

            DescriptorProtos.FileDescriptorSet.Builder protoBuilder = DescriptorProtos.FileDescriptorSet.newBuilder();

            for (Descriptors.FileDescriptor file : fileTopologicalOrder) {
                protoBuilder.addFile(strip(file, messageNames, enumNames));
            }
            return protoBuilder.build();
        }

        private DescriptorProtos.FileDescriptorProto strip(
                Descriptors.FileDescriptor file,
                Set<String> requiredMessageNames,
                Set<String> requiredEnumNames
        ) {
            DescriptorProtos.FileDescriptorProto.Builder fileProto = file.toProto().toBuilder();
            fileProto.clearService();
            fileProto.clearExtension();

            String packagePrefix = fileProto.getPackage().isEmpty() ? "" : fileProto.getPackage() + '.';

            List<DescriptorProto> messageTypeList = fileProto.getMessageTypeList();
            fileProto.clearMessageType();
            fileProto.addAllMessageType(
                    messageTypeList.stream()
                            .filter(m -> requiredMessageNames.contains(packagePrefix + m.getName()))
                            .map(this::stripUnknownExtensions)
                            .collect(Collectors.toList())
            );

            List<EnumDescriptorProto.Builder> enumTypeList = fileProto.getEnumTypeBuilderList();
            fileProto.clearEnumType();
            fileProto.addAllEnumType(
                    enumTypeList.stream()
                            .filter(e -> requiredEnumNames.contains(packagePrefix + e.getName()))
                            .map(EnumDescriptorProto.Builder::build)
                            .collect(Collectors.toList())
            );
            return fileProto.build();
        }

        private DescriptorProto stripUnknownExtensions(DescriptorProto messageDescriptor) {
            DescriptorProto.Builder builder = messageDescriptor.toBuilder();
            stripUnknownExtensions(builder);
            return builder.build();
        }

        private void stripUnknownExtensions(DescriptorProto.Builder builder) {
            stripUnknownExtensionsFromOptions(builder.getOptionsBuilder());

            for (FieldDescriptorProto.Builder field : builder.getFieldBuilderList()) {
                if (field.hasOptions()) {
                    stripUnknownExtensionsFromOptions(field.getOptionsBuilder());
                }
            }

            for (OneofDescriptorProto.Builder oneof : builder.getOneofDeclBuilderList()) {
                if (oneof.hasOptions()) {
                    stripUnknownExtensionsFromOptions(oneof.getOptionsBuilder());
                }
            }

            for (DescriptorProto.Builder nestedTypeProto : builder.getNestedTypeBuilderList()) {
                stripUnknownExtensions(nestedTypeProto);
            }

            for (EnumDescriptorProto.Builder enumProto : builder.getEnumTypeBuilderList()) {
                if (enumProto.hasOptions()) {
                    stripUnknownExtensionsFromOptions(enumProto.getOptionsBuilder());
                }
                for (EnumValueDescriptorProto.Builder enumValue : enumProto.getValueBuilderList()) {
                    if (enumValue.hasOptions()) {
                        stripUnknownExtensionsFromOptions(enumValue.getOptionsBuilder());
                    }
                }
            }
        }

        private void stripUnknownExtensionsFromOptions(Builder options) {
            for (FieldDescriptor field : options.getAllFields().keySet()) {
                if (field.isExtension() && field.getFile() != SYSTEM_EXTENSION_FILE) {
                    options.clearField(field);
                }
            }
        }

        private void traverseDependencies(
                Descriptors.FileDescriptor file,
                Set<Descriptors.FileDescriptor> visitedFiles,
                List<Descriptors.FileDescriptor> fileTopologicalOrder
        ) {
            if (!visitedFiles.add(file)) {
                return;
            }
            for (Descriptors.FileDescriptor dependency : file.getDependencies()) {
                traverseDependencies(dependency, visitedFiles, fileTopologicalOrder);
            }
            fileTopologicalOrder.add(file);
        }

        private void addField(FieldDescriptor field) {
            if (field.getType() == Type.MESSAGE) {
                addDescriptor(field.getMessageType());
            }

            if (field.getType() == Type.ENUM) {
                addEnumDescriptor(field.getEnumType());
            }
        }

        private void addEnumDescriptor(EnumDescriptor enumType) {
            if (!allEnumDescriptors.add(enumType)) {
                return;
            }

            addAllContainingTypes(enumType.getContainingType());
        }

        private void addAllContainingTypes(@Nullable Descriptor descriptor) {
            while (descriptor != null) {
                addDescriptor(descriptor);
                descriptor = descriptor.getContainingType();
            }
        }
    }
}
