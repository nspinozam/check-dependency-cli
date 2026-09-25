package checker

import (
	"context"
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
)

type Definition struct {
	Dependencies []Dependency `yaml:"dependencies"`
}

type Dependency struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Name       string `yaml:"name"`
	Namespace  string `yaml:"namespace,omitempty"`
	Key        string `yaml:"key,omitempty"`
}

func Run(fileName, kubeconfig, contextName string) error {
	definition, err := loadDefinition(fileName)
	if err != nil {
		return err
	}
	if len(definition.Dependencies) == 0 {
		return errors.New("no dependencies found")
	}

	config, err := kubeConfig(kubeconfig, contextName)
	if err != nil {
		return fmt.Errorf("create Kubernetes config: %w", err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create Kubernetes client: %w", err)
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return fmt.Errorf("create Kubernetes discovery client: %w", err)
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient))

	ctx := context.Background()
	for _, dependency := range definition.Dependencies {
		if err := validateDependency(ctx, dynamicClient, mapper, dependency); err != nil {
			return err
		}
		fmt.Printf("OK %s/%s %s\n", dependency.Kind, dependency.Namespace, dependency.Name)
	}
	return nil
}

func loadDefinition(fileName string) (Definition, error) {
	contents, err := os.ReadFile(fileName)
	if err != nil {
		return Definition{}, fmt.Errorf("read %s: %w", fileName, err)
	}
	var definition Definition
	if err := yaml.Unmarshal(contents, &definition); err != nil {
		return Definition{}, fmt.Errorf("parse %s: %w", fileName, err)
	}
	for index, dependency := range definition.Dependencies {
		if dependency.APIVersion == "" || dependency.Kind == "" || dependency.Name == "" {
			return Definition{}, fmt.Errorf("dependency %d requires apiVersion, kind and name", index+1)
		}
	}
	return definition, nil
}

func kubeConfig(kubeconfig, contextName string) (*rest.Config, error) {
	if kubeconfig == "" {
		if config, err := rest.InClusterConfig(); err == nil {
			return config, nil
		}
	}
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		loadingRules.ExplicitPath = kubeconfig
	}
	overrides := &clientcmd.ConfigOverrides{CurrentContext: contextName}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides).ClientConfig()
}

func validateDependency(ctx context.Context, client dynamic.Interface, mapper *restmapper.DeferredDiscoveryRESTMapper, dependency Dependency) error {
	groupVersion, err := schema.ParseGroupVersion(dependency.APIVersion)
	if err != nil {
		return fmt.Errorf("dependency %s/%s: invalid apiVersion: %w", dependency.Kind, dependency.Name, err)
	}
	mapping, err := mapper.RESTMapping(groupVersion.WithKind(dependency.Kind).GroupKind(), groupVersion.Version)
	if err != nil {
		return fmt.Errorf("map %s/%s: %w", dependency.Kind, dependency.Name, err)
	}
	var resource dynamic.ResourceInterface
	if mapping.Scope.Name() == "namespace" {
		resource = client.Resource(mapping.Resource).Namespace(dependency.Namespace)
	} else {
		resource = client.Resource(mapping.Resource)
	}
	object, err := resource.Get(ctx, dependency.Name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("missing %s/%s", dependency.Kind, dependency.Name)
		}
		return fmt.Errorf("get %s/%s: %w", dependency.Kind, dependency.Name, err)
	}
	if dependency.Key != "" {
		if err := validateSecretKey(object, dependency.Key); err != nil {
			return fmt.Errorf("%s/%s: %w", dependency.Kind, dependency.Name, err)
		}
	}
	if dependency.Kind == "Component" && dependency.APIVersion == "dapr.io/v1alpha1" {
		return validateDaprSecretReferences(ctx, client, mapper, object, dependency.Namespace)
	}
	return nil
}

func validateSecretKey(object *unstructured.Unstructured, key string) error {
	data, found, err := unstructured.NestedStringMap(object.Object, "data")
	if err != nil {
		return fmt.Errorf("read secret data: %w", err)
	}
	if !found {
		return fmt.Errorf("missing key %q", key)
	}
	if _, found := data[key]; !found {
		return fmt.Errorf("missing key %q", key)
	}
	return nil
}

func validateDaprSecretReferences(ctx context.Context, client dynamic.Interface, mapper *restmapper.DeferredDiscoveryRESTMapper, component *unstructured.Unstructured, namespace string) error {
	metadata, found, err := unstructured.NestedSlice(component.Object, "spec", "metadata")
	if err != nil || !found {
		return err
	}
	for _, item := range metadata {
		entry, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		reference, ok := entry["secretKeyRef"].(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := reference["name"].(string)
		key, _ := reference["key"].(string)
		if name == "" || key == "" {
			return fmt.Errorf("invalid secretKeyRef in component")
		}
		secret := Dependency{APIVersion: "v1", Kind: "Secret", Name: name, Namespace: namespace, Key: key}
		if err := validateDependency(ctx, client, mapper, secret); err != nil {
			return fmt.Errorf("component secret reference: %w", err)
		}
	}
	return nil
}
