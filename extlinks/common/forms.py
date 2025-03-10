from datetime import timedelta
from dateutil.relativedelta import relativedelta

from django import forms


class DateInput(forms.DateInput):
    # Creating a custom widget because default DateInput doesn't use
    # input type="date"
    input_type = "date"


class FilterForm(forms.Form):

    start_date = forms.DateField(
        required=False,
        label="Start date:",
        widget=DateInput(attrs={"class": "form-control"}),
    )
    end_date = forms.DateField(
        required=False,
        label="End date:",
        widget=DateInput(attrs={"class": "form-control"}),
    )

    limit_to_user_list = forms.BooleanField(required=False)

    namespace_id = forms.IntegerField(
        required=False,
        label="Namespace ID:",
        widget=forms.NumberInput(
            attrs={"class": "form-control", "style": "width: 6rem;"}
        ),
    )

    exclude_bots = forms.BooleanField(required=False)

    def clean_start_date(self):
        """
        This is automatically called by Django when validating this field.
        Modify the start date to return the first day of its month.
        """
        start_date = self.cleaned_data.get("start_date")

        if not start_date:
            return None

        return start_date.replace(day=1)


    def clean_end_date(self):
        """
        This is automatically called by Django when validating this field.
        Modify the end date to return the last day of its month.
        """
        end_date = self.cleaned_data.get("end_date")

        if not end_date:
            return None

        next_month = end_date.replace(day=1) + relativedelta(months=1)

        return next_month - timedelta(days=1)
