// Kiwi uses flags and a single config file for configuration.
// A config file is stored in .txtpb format and contains the values that can be set via flags.

package config

import (
	"encoding/base64"
	"flag"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	kiwipb "github.com/nobletooth/kiwi/proto"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// skippedProtobufFlags is the list of command line flags on which the protobuf check is disabled.
var skippedProtobufFlags = []string{"print_version", "config_file"}

// protobufValueToString converts a protobuf field value to its string representation suitable for flag setting.
func protobufValueToString(fd protoreflect.FieldDescriptor, v protoreflect.Value) (string, error) {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return strconv.FormatBool(v.Bool()), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return strconv.FormatInt(v.Int(), 10), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return strconv.FormatUint(v.Uint(), 10), nil
	case protoreflect.FloatKind:
		return strconv.FormatFloat(v.Float(), 'g', -1, 32), nil
	case protoreflect.DoubleKind:
		return strconv.FormatFloat(v.Float(), 'g', -1, 64), nil
	case protoreflect.StringKind:
		return v.String(), nil
	case protoreflect.BytesKind:
		return base64.StdEncoding.EncodeToString(v.Bytes()), nil
	case protoreflect.EnumKind:
		// Use enum name for readability.
		if ev := fd.Enum().Values().ByNumber(v.Enum()); ev != nil {
			return string(ev.Name()), nil
		}
		return strconv.FormatInt(int64(v.Enum()), 10), nil
	case protoreflect.MessageKind:
		fullName := string(fd.Message().FullName())
		switch fullName {
		case "google.protobuf.Timestamp":
			return v.Message().Interface().(*timestamppb.Timestamp).AsTime().Format(time.RFC3339Nano), nil
		case "google.protobuf.Duration":
			return v.Message().Interface().(*durationpb.Duration).AsDuration().String(), nil
		default:
			// If a message itself has a flag_name, we allow its prototext form.
			if proto.HasExtension(fd.Options(), kiwipb.E_FlagName) {
				return prototext.MarshalOptions{Multiline: false}.Format(v.Message().Interface()), nil
			}
			return "", fmt.Errorf("unsupported message leaf: %s", fullName)
		}
	default:
		return "", fmt.Errorf("unsupported kind: %v", fd.Kind())
	}
}

// collectAndRegisterFlags collects all registered flags with their values from the given protobuf message.
// The collected flags are put inside the given `flags` variable.
// Each protobuf field can have a flag annotation attached to it that specifies its command line flag name.
func collectAndRegisterFlags(flags map[ /*flagName*/ string] /*flagValue*/ string, m protoreflect.Message) error {
	var err error
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		// Oneof blocks appear as regular set fields in Range. Lists/maps are not supported by design.
		if fd.IsList() || fd.IsMap() {
			err = fmt.Errorf("repeated/map not supported: %s", fd.FullName())
			return false
		}
		hasFlagName := proto.HasExtension(fd.Options(), kiwipb.E_FlagName)
		// Recurse into nested messages that do not carry a flag_name themselves.
		if fd.Kind() == protoreflect.MessageKind && !hasFlagName {
			if !m.Has(fd) {
				return true
			}
			err = collectAndRegisterFlags(flags, v.Message())
			return err == nil
		}
		// If annotated, convert to string and set the target flag.
		if hasFlagName && m.Has(fd) {
			var flagName string
			{ // Get the flag name from the annotation.
				ext := proto.GetExtension(fd.Options(), kiwipb.E_FlagName)
				if extString, ok := ext.(string); ok {
					flagName = extString
				}
			}
			// Convert the protobuf value to string.
			stringValue, convErr := protobufValueToString(fd, v)
			if convErr != nil {
				err = fmt.Errorf("failed to convert %s: %w", fd.FullName(), convErr)
				return false
			}
			// Check for duplicate flag entries.
			if _, alreadyExists := flags[flagName]; alreadyExists {
				err = fmt.Errorf("flag '%s' has multiple entries in txtpb config: '%s'", flagName, fd.FullName())
				return false
			}
			flags[flagName] = stringValue
		}
		// Skip other fields.
		return true
	})
	return err
}

// setConfigFlags sets all the filled flags in the given `conf` to the global flag variables.
func setConfigFlags(conf *kiwipb.Config) error {
	registeredFlags := make(map[ /*flagName*/ string] /*flagValue*/ string)
	if err := collectAndRegisterFlags(registeredFlags, conf.ProtoReflect()); err != nil {
		return fmt.Errorf("failed to collect flags: %w", err)
	}
	for flagName, flagValue := range registeredFlags {
		if setErr := flag.Set(flagName, flagValue); setErr != nil {
			return fmt.Errorf("failed to set flag %s: %w", flagName, setErr)
		}
	}
	return nil
}

// getDefinedFlags returns the set of defined flags inside the given protobuf message schema.
func getDefinedFlags(md protoreflect.MessageDescriptor) (map[ /*flagName*/ string]struct{}, error) {
	flagSet := make(map[ /*flagName*/ string]struct{})
	var walkFields func(md protoreflect.MessageDescriptor) error
	walkFields = func(md protoreflect.MessageDescriptor) error {
		for fieldIdx := 0; fieldIdx < md.Fields().Len(); fieldIdx++ {
			fd := md.Fields().Get(fieldIdx)
			if fd.IsList() || fd.IsMap() {
				continue // Skip repeated/map fields.
			}
			if proto.HasExtension(fd.Options(), kiwipb.E_FlagName) {
				ext := proto.GetExtension(fd.Options(), kiwipb.E_FlagName)
				if flagName, ok := ext.(string); ok && flagName != "" {
					if _, exists := flagSet[flagName]; exists {
						return fmt.Errorf("duplicate flag name '%s' in config: %s", flagName, fd.FullName())
					}
					flagSet[flagName] = struct{}{}
				}
			}
			if fd.Kind() == protoreflect.MessageKind {
				if err := walkFields(fd.Message()); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walkFields(md); err != nil {
		return nil, err
	}
	return flagSet, nil
}

// CollectUnregisteredFlags collects all flags that haven't been registered in the protobuf config.
// An error exists in the results corresponding to each unregistered flag.
func CollectUnregisteredFlags() []error {
	// Use the Config message descriptor from the generated proto package
	definedFlags, err := getDefinedFlags(kiwipb.File_config_proto.Messages().ByName("Config"))
	if err != nil {
		return []error{err}
	}
	errs := make([]error, 0)
	flag.VisitAll(func(f *flag.Flag) {
		if strings.HasPrefix(f.Name, "test.") { // Skip test flags.
			return
		}
		if slices.Contains(skippedProtobufFlags, f.Name) {
			return
		}
		if _, flagHasConfigEntry := definedFlags[f.Name]; !flagHasConfigEntry {
			errs = append(errs, fmt.Errorf("flag '%s' has not been defined in protobuf config", f.Name))
		}
	})
	return errs
}
