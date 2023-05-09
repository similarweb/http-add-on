package routing

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func toNamespacedName(obj client.Object) types.NamespacedName {
	return types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}
}

func applyContext(ctx context.Context, f func(ctx context.Context) error) func() error {
	return func() error {
		return f(ctx)
	}
}
