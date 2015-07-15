from django.views.generic import TemplateView

from kadi import events
from kadi.events.views import BaseView
from Chandra.Time import DateTime


class LoadSegView(BaseView, TemplateView):
    template_name = 'load_seg_changes/index.html'

    def get_context_data(self, **kwargs):
        # Call the base implementation first to get a context
        context = super(LoadSegView, self).get_context_data(**kwargs)

        context['errors'] = []

        start_time = self.request.GET.get('start_time', None)
        stop_time = self.request.GET.get('stop_time', None)
        if start_time is not None:
            try:
                start_time = DateTime(start_time)
                start_time.date
            except:
                start_time = None
                context['start_time_date'] = ''
        if stop_time is not None:
            try:
                stop_time = DateTime(stop_time)
                stop_time.date
            except:
                stop_time = None
                context['stop_time_date'] = ''

        from load_seg_changes import find_changes
        changes = find_changes(start=start_time, stop=stop_time)

        context['start_time_date'] = DateTime(changes['start']).date
        context['stop_time_date'] = DateTime(changes['stop']).date
        context['htmldiff'] = changes['htmldiff']

        return context
