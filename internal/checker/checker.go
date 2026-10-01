package checker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
)

type Definition struct {
	Dependencies []Dependency `yaml:"dependencies"`
}

type Dependency struct {
	APIVersion  string `yaml:"apiVersion"`
	Kind        string `yaml:"kind"`
	Name        string `yaml:"name"`
	Namespace   string `yaml:"namespace,omitempty"`
	Key         string `yaml:"key,omitempty"`
	SecretStore string `yaml:"secretStore,omitempty"`
}

func Run(fileName, kubeconfig, contextName, daprHTTPAddress string) error {
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
	kubernetesClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create Kubernetes typed client: %w", err)
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return fmt.Errorf("create Kubernetes discovery client: %w", err)
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient))

	ctx := context.Background()
	var sidecar *daprSidecar
	for _, dependency := range definition.Dependencies {
		address := daprHTTPAddress
		if dependency.APIVersion == "dapr.io/v1alpha1" && dependency.Kind == "DaprSecret" {
			if address == "" && sidecar == nil {
				sidecar, err = createDaprSidecar(ctx, kubernetesClient, dependency.Namespace)
				if err != nil {
					return err
				}
			}
			if address == "" {
				address = sidecar.address
			}
		}
		if err := validateDependency(ctx, dynamicClient, kubernetesClient, mapper, address, dependency); err != nil {
			return err
		}
		fmt.Printf("OK %s/%s %s\n", dependency.Kind, dependency.Namespace, dependency.Name)
	}
	return nil
}

type daprSidecar struct {
	client    kubernetes.Interface
	namespace string
	name      string
	service   string
	address   string
}

func createDaprSidecar(ctx context.Context, client kubernetes.Interface, namespace string) (*daprSidecar, error) {
	if namespace == "" {
		namespace = "default"
	}
	labels := map[string]string{"check-dependency/component": "dapr-sidecar"}
	pods := client.CoreV1().Pods(namespace)
	pod, err := pods.Get(ctx, "check-dependency-dapr", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		pod, err = pods.Create(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "check-dependency-dapr",
				Namespace: namespace,
				Labels:    labels,
				Annotations: map[string]string{
					"dapr.io/enabled":                  "true",
					"dapr.io/app-id":                   "check-dependency",
					"dapr.io/app-port":                 "3501",
					"dapr.io/sidecar-listen-addresses": "0.0.0.0",
				},
			},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers: []corev1.Container{{
					Name:    "holder",
					Image:   "busybox:1.36",
					Command: []string{"sleep", "3600"},
				}},
			},
		}, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, fmt.Errorf("get or create Dapr sidecar pod: %w", err)
	}
	services := client.CoreV1().Services(namespace)
	service, err := services.Get(ctx, "check-dependency-dapr", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		service, err = services.Create(ctx, &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "check-dependency-dapr",
				Namespace: namespace,
			},
			Spec: corev1.ServiceSpec{
				Selector: labels,
				Ports: []corev1.ServicePort{{
					Name:       "http",
					Port:       3500,
					TargetPort: intstr.FromInt(3500),
				}},
			},
		}, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, fmt.Errorf("get or create Dapr sidecar service: %w", err)
	}
	sidecar := &daprSidecar{
		client:    client,
		namespace: namespace,
		name:      pod.Name,
		service:   service.Name,
		address:   fmt.Sprintf("http://%s.%s.svc.cluster.local:3500", service.Name, namespace),
	}
	if err := sidecar.wait(ctx); err != nil {
		return nil, err
	}
	return sidecar, nil
}

func (sidecar *daprSidecar) wait(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		pod, err := sidecar.client.CoreV1().Pods(sidecar.namespace).Get(ctx, sidecar.name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get Dapr sidecar pod: %w", err)
		}
		if pod.Status.Phase == corev1.PodFailed {
			return fmt.Errorf("Dapr sidecar pod failed")
		}
		if pod.Status.Phase == corev1.PodRunning {
			if err := checkDaprHealth(ctx, sidecar.client, sidecar.namespace, sidecar.address); err == nil {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for Dapr sidecar pod: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func checkDaprHealth(ctx context.Context, client kubernetes.Interface, namespace, address string) error {
	_, err := curlDapr(ctx, client, namespace, strings.TrimRight(address, "/")+"/v1.0/healthz", true)
	return err
}

func curlDapr(ctx context.Context, client kubernetes.Interface, namespace, endpoint string, retry bool) ([]byte, error) {
	command := "curl --fail --silent --show-error --max-time 10 \"$DAPR_URL\""
	if retry {
		command = "until " + command + "; do sleep 1; done"
	}
	pod, err := client.CoreV1().Pods(namespace).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "check-dependency-curl-", Namespace: namespace},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:    "curl",
				Image:   "curlimages/curl:8.11.1",
				Command: []string{"sh", "-c", command},
				Env:     []corev1.EnvVar{{Name: "DAPR_URL", Value: endpoint}},
			}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create Dapr curl pod: %w", err)
	}
	defer func() {
		_ = client.CoreV1().Pods(namespace).Delete(context.Background(), pod.Name, metav1.DeleteOptions{})
	}()
	for {
		pod, err = client.CoreV1().Pods(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("get Dapr curl pod: %w", err)
		}
		switch pod.Status.Phase {
		case corev1.PodSucceeded:
			logs, err := client.CoreV1().Pods(namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: "curl"}).Stream(ctx)
			if err != nil {
				return nil, fmt.Errorf("get Dapr curl pod logs: %w", err)
			}
			defer logs.Close()
			body, err := io.ReadAll(logs)
			if err != nil {
				return nil, fmt.Errorf("read Dapr curl pod logs: %w", err)
			}
			return body, nil
		case corev1.PodFailed:
			return nil, fmt.Errorf("Dapr curl pod failed")
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for Dapr curl pod: %w", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
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

func validateDependency(ctx context.Context, client dynamic.Interface, kubernetesClient kubernetes.Interface, mapper *restmapper.DeferredDiscoveryRESTMapper, daprHTTPAddress string, dependency Dependency) error {
	if dependency.APIVersion == "dapr.io/v1alpha1" && dependency.Kind == "DaprSecret" {
		return validateDaprSecretFromPod(ctx, kubernetesClient, daprHTTPAddress, dependency)
	}
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
		return validateDaprSecretReferences(ctx, client, kubernetesClient, mapper, object, dependency.Namespace)
	}
	return nil
}

func validateDaprSecretFromPod(ctx context.Context, client kubernetes.Interface, daprHTTPAddress string, dependency Dependency) error {
	namespace := dependency.Namespace
	if namespace == "" {
		namespace = "default"
	}
	if dependency.SecretStore == "" {
		return fmt.Errorf("dapr secret %s/%s requires secretStore", dependency.Namespace, dependency.Name)
	}
	if dependency.Key == "" {
		return fmt.Errorf("dapr secret %s/%s requires key", dependency.Namespace, dependency.Name)
	}
	if daprHTTPAddress == "" {
		return errors.New("dapr HTTP address is required")
	}
	endpoint := strings.TrimRight(daprHTTPAddress, "/") + "/v1.0/secrets/" + url.PathEscape(dependency.SecretStore) + "/" + url.PathEscape(dependency.Name)
	body, err := curlDapr(ctx, client, namespace, endpoint, false)
	if err != nil {
		return fmt.Errorf("get Dapr secret %s/%s: %w", dependency.SecretStore, dependency.Name, err)
	}
	var secret map[string]json.RawMessage
	if err := json.Unmarshal(body, &secret); err != nil {
		return fmt.Errorf("decode Dapr secret %s/%s: %w", dependency.SecretStore, dependency.Name, err)
	}
	if _, found := secret[dependency.Key]; !found {
		return fmt.Errorf("dapr secret %s/%s: missing key %q", dependency.SecretStore, dependency.Name, dependency.Key)
	}
	return nil
}

func validateDaprSecret(ctx context.Context, daprHTTPAddress string, dependency Dependency) error {
	if dependency.SecretStore == "" {
		return fmt.Errorf("dapr secret %s/%s requires secretStore", dependency.Namespace, dependency.Name)
	}
	if dependency.Key == "" {
		return fmt.Errorf("dapr secret %s/%s requires key", dependency.Namespace, dependency.Name)
	}
	if daprHTTPAddress == "" {
		return errors.New("dapr HTTP address is required")
	}
	endpoint := strings.TrimRight(daprHTTPAddress, "/") + "/v1.0/secrets/" + url.PathEscape(dependency.SecretStore) + "/" + url.PathEscape(dependency.Name)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("create Dapr secret request: %w", err)
	}
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return fmt.Errorf("get Dapr secret %s/%s: %w", dependency.SecretStore, dependency.Name, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("get Dapr secret %s/%s: unexpected HTTP status %s", dependency.SecretStore, dependency.Name, response.Status)
	}
	var secret map[string]json.RawMessage
	if err := json.NewDecoder(response.Body).Decode(&secret); err != nil {
		return fmt.Errorf("decode Dapr secret %s/%s: %w", dependency.SecretStore, dependency.Name, err)
	}
	if _, found := secret[dependency.Key]; !found {
		return fmt.Errorf("dapr secret %s/%s: missing key %q", dependency.SecretStore, dependency.Name, dependency.Key)
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

func validateDaprSecretReferences(ctx context.Context, client dynamic.Interface, kubernetesClient kubernetes.Interface, mapper *restmapper.DeferredDiscoveryRESTMapper, component *unstructured.Unstructured, namespace string) error {
	metadata, found, err := unstructured.NestedSlice(component.Object, "spec", "metadata")
	if err != nil || !found {
		return err
	}
	for index, item := range metadata {
		entry, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		name, key, found, err := daprSecretReference(entry)
		if err != nil {
			return fmt.Errorf("invalid secretKeyRef in component metadata %d: %w", index+1, err)
		}
		if !found {
			continue
		}
		secret := Dependency{APIVersion: "v1", Kind: "Secret", Name: name, Namespace: namespace, Key: key}
		if err := validateDependency(ctx, client, kubernetesClient, mapper, "", secret); err != nil {
			return fmt.Errorf("component secret reference: %w", err)
		}
	}
	return nil
}

func daprSecretReference(entry map[string]interface{}) (string, string, bool, error) {
	reference, found, err := unstructured.NestedMap(entry, "secretKeyRef")
	if err != nil {
		return "", "", false, err
	}
	if !found {
		return "", "", false, nil
	}
	name, nameOK := reference["name"].(string)
	key, keyOK := reference["key"].(string)
	if !nameOK || !keyOK || name == "" || key == "" {
		return "", "", true, errors.New("name and key are required")
	}
	return name, key, true, nil
}
